#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SeenState {
    Active = 0x0_u8,
    Filled = 0x1_u8,
    Canceled = 0x2_u8,
    Rejected = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for SeenState {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Active,
            0x1_u8 => Self::Filled,
            0x2_u8 => Self::Canceled,
            0x3_u8 => Self::Rejected,
            _ => Self::NullVal,
        }
    }
}
impl From<SeenState> for u8 {
    #[inline]
    fn from(v: SeenState) -> Self {
        match v {
            SeenState::Active => 0x0_u8,
            SeenState::Filled => 0x1_u8,
            SeenState::Canceled => 0x2_u8,
            SeenState::Rejected => 0x3_u8,
            SeenState::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for SeenState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Active" => Ok(Self::Active),
            "Filled" => Ok(Self::Filled),
            "Canceled" => Ok(Self::Canceled),
            "Rejected" => Ok(Self::Rejected),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for SeenState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Active => write!(f, "Active"),
            Self::Filled => write!(f, "Filled"),
            Self::Canceled => write!(f, "Canceled"),
            Self::Rejected => write!(f, "Rejected"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
