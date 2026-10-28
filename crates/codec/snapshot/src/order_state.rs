#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrderState {
    New = 0x0_u8,
    Partial = 0x1_u8,
    Filled = 0x2_u8,
    Canceled = 0x3_u8,
    Rejected = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrderState {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::New,
            0x1_u8 => Self::Partial,
            0x2_u8 => Self::Filled,
            0x3_u8 => Self::Canceled,
            0x4_u8 => Self::Rejected,
            _ => Self::NullVal,
        }
    }
}
impl From<OrderState> for u8 {
    #[inline]
    fn from(v: OrderState) -> Self {
        match v {
            OrderState::New => 0x0_u8,
            OrderState::Partial => 0x1_u8,
            OrderState::Filled => 0x2_u8,
            OrderState::Canceled => 0x3_u8,
            OrderState::Rejected => 0x4_u8,
            OrderState::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrderState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "New" => Ok(Self::New),
            "Partial" => Ok(Self::Partial),
            "Filled" => Ok(Self::Filled),
            "Canceled" => Ok(Self::Canceled),
            "Rejected" => Ok(Self::Rejected),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrderState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::New => write!(f, "New"),
            Self::Partial => write!(f, "Partial"),
            Self::Filled => write!(f, "Filled"),
            Self::Canceled => write!(f, "Canceled"),
            Self::Rejected => write!(f, "Rejected"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
