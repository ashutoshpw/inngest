//! Pure opcode grouping and capability decisions for the isolated bridge spike.
//!
//! Wire values are versioned protocol tags. They intentionally do not depend on
//! Go's enum representation or ordering at the FFI/process boundary.

use std::fmt;

pub const MAX_OPCODES: usize = 65_536;

/// Version-1 wire tags in the original Go opcode order, assigned explicitly.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum OpcodeTag {
    None = 0,
    Step = 1,
    StepRun = 2,
    StepError = 3,
    StepPlanned = 4,
    Sleep = 5,
    WaitForEvent = 6,
    InvokeFunction = 7,
    AIGateway = 8,
    Gateway = 9,
    WaitForSignal = 10,
    RunComplete = 11,
    StepFailed = 12,
    SyncRunComplete = 13,
    DiscoveryRequest = 14,
    DeferAdd = 15,
    DeferAbort = 16,
}

impl TryFrom<u8> for OpcodeTag {
    type Error = UnknownOpcodeTag;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Step),
            2 => Ok(Self::StepRun),
            3 => Ok(Self::StepError),
            4 => Ok(Self::StepPlanned),
            5 => Ok(Self::Sleep),
            6 => Ok(Self::WaitForEvent),
            7 => Ok(Self::InvokeFunction),
            8 => Ok(Self::AIGateway),
            9 => Ok(Self::Gateway),
            10 => Ok(Self::WaitForSignal),
            11 => Ok(Self::RunComplete),
            12 => Ok(Self::StepFailed),
            13 => Ok(Self::SyncRunComplete),
            14 => Ok(Self::DiscoveryRequest),
            15 => Ok(Self::DeferAdd),
            16 => Ok(Self::DeferAbort),
            other => Err(UnknownOpcodeTag(other)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnknownOpcodeTag(pub u8);

impl fmt::Display for UnknownOpcodeTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "unknown opcode tag {}", self.0)
    }
}

impl std::error::Error for UnknownOpcodeTag {}

impl OpcodeTag {
    pub const fn is_priority(self) -> bool {
        matches!(self, Self::WaitForEvent | Self::DeferAdd | Self::DeferAbort)
    }

    pub const fn is_lazy(self) -> bool {
        matches!(self, Self::DeferAdd | Self::DeferAbort)
    }

    pub const fn has_plan_capability(self) -> bool {
        matches!(
            self,
            Self::AIGateway
                | Self::Gateway
                | Self::InvokeFunction
                | Self::Sleep
                | Self::StepPlanned
                | Self::WaitForEvent
                | Self::WaitForSignal
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Projection {
    pub priority_indices: Vec<u32>,
    pub other_indices: Vec<u32>,
    pub non_lazy_indices: Vec<u32>,
    pub history_group: bool,
    pub has_plan_op: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KernelError {
    TooManyOpcodes,
    AllocationFailed,
}

impl fmt::Display for KernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyOpcodes => formatter.write_str("opcode count exceeds protocol limit"),
            Self::AllocationFailed => formatter.write_str("bounded projection allocation failed"),
        }
    }
}

impl std::error::Error for KernelError {}

/// Compute stable original-position groups and their non-lazy subsequence.
pub fn project(tags: &[OpcodeTag]) -> Result<Projection, KernelError> {
    if tags.len() > MAX_OPCODES {
        return Err(KernelError::TooManyOpcodes);
    }

    let mut priority_indices = Vec::new();
    let mut other_indices = Vec::new();
    let mut non_lazy_indices = Vec::new();
    priority_indices
        .try_reserve_exact(tags.len())
        .map_err(|_| KernelError::AllocationFailed)?;
    other_indices
        .try_reserve_exact(tags.len())
        .map_err(|_| KernelError::AllocationFailed)?;
    non_lazy_indices
        .try_reserve_exact(tags.len())
        .map_err(|_| KernelError::AllocationFailed)?;

    let mut non_lazy_count = 0usize;
    let mut has_plan_op = false;
    for (index, tag) in tags.iter().copied().enumerate() {
        let index = index as u32;
        if tag.is_priority() {
            priority_indices.push(index);
        } else {
            other_indices.push(index);
        }
        if !tag.is_lazy() {
            non_lazy_count += 1;
        }
        has_plan_op |= tag.has_plan_capability();
    }

    for index in priority_indices.iter().chain(other_indices.iter()).copied() {
        if !tags[index as usize].is_lazy() {
            non_lazy_indices.push(index);
        }
    }

    Ok(Projection {
        priority_indices,
        other_indices,
        non_lazy_indices,
        history_group: non_lazy_count > 1,
        has_plan_op,
    })
}

#[cfg(test)]
mod tests {
    use super::{KernelError, MAX_OPCODES, OpcodeTag as T, project};

    #[test]
    fn empty_is_distinct_from_a_single_none_opcode() {
        assert_eq!(project(&[]).unwrap(), Default::default());

        let one = project(&[T::None]).unwrap();
        assert_eq!(one.other_indices, [0]);
        assert_eq!(one.non_lazy_indices, [0]);
        assert!(!one.history_group);
        assert!(!one.has_plan_op);
    }

    #[test]
    fn stable_groups_and_non_lazy_positions_follow_original_indices() {
        let result = project(&[
            T::Sleep,
            T::WaitForEvent,
            T::RunComplete,
            T::DeferAdd,
            T::StepRun,
            T::DeferAbort,
        ])
        .unwrap();

        assert_eq!(result.priority_indices, [1, 3, 5]);
        assert_eq!(result.other_indices, [0, 2, 4]);
        assert_eq!(result.non_lazy_indices, [1, 0, 2, 4]);
        assert!(result.history_group);
        assert!(result.has_plan_op);
    }

    #[test]
    fn lazy_only_has_priority_but_no_history_or_plan_capability() {
        let result = project(&[T::DeferAdd, T::DeferAbort]).unwrap();

        assert_eq!(result.priority_indices, [0, 1]);
        assert!(result.non_lazy_indices.is_empty());
        assert!(!result.history_group);
        assert!(!result.has_plan_op);
    }

    #[test]
    fn rejects_counts_outside_the_wire_bound() {
        let tags = vec![T::None; MAX_OPCODES + 1];
        assert_eq!(project(&tags), Err(KernelError::TooManyOpcodes));
    }
}
