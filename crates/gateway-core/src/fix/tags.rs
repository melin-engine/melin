//! FIX 4.2 tag constants and message type codes.

// --- Standard header/trailer tags ---
pub const BEGIN_STRING: u32 = 8;
pub const BODY_LENGTH: u32 = 9;
pub const MSG_TYPE: u32 = 35;
pub const SENDER_COMP_ID: u32 = 49;
pub const TARGET_COMP_ID: u32 = 56;
pub const MSG_SEQ_NUM: u32 = 34;
pub const SENDING_TIME: u32 = 52;
pub const CHECK_SUM: u32 = 10;

// --- Session messages ---
pub const HEART_BT_INT: u32 = 108;
pub const TEST_REQ_ID: u32 = 112;
pub const ENCRYPT_METHOD: u32 = 98;
pub const TEXT: u32 = 58;
pub const POSS_DUP_FLAG: u32 = 43;
pub const ORIG_SENDING_TIME: u32 = 122;
pub const BEGIN_SEQ_NO: u32 = 7;
pub const END_SEQ_NO: u32 = 16;
pub const NEW_SEQ_NO: u32 = 36;
pub const GAP_FILL_FLAG: u32 = 123;

// --- Order entry tags ---
pub const ACCOUNT: u32 = 1;
pub const CL_ORD_ID: u32 = 11;
pub const ORIG_CL_ORD_ID: u32 = 41;
pub const EXEC_ID: u32 = 17;
pub const EXEC_TRANS_TYPE: u32 = 20;
pub const EXEC_TYPE: u32 = 150;
pub const ORD_STATUS: u32 = 39;
pub const ORD_TYPE: u32 = 40;
pub const ORDER_QTY: u32 = 38;
pub const PRICE: u32 = 44;
pub const STOP_PX: u32 = 99;
pub const SIDE: u32 = 54;
pub const SYMBOL: u32 = 55;
pub const TIME_IN_FORCE: u32 = 59;
pub const ORDER_ID: u32 = 37;
pub const LAST_SHARES: u32 = 32;
pub const LAST_PX: u32 = 31;
pub const LEAVES_QTY: u32 = 151;
pub const CUM_QTY: u32 = 14;
pub const AVG_PX: u32 = 6;
pub const ORD_REJ_REASON: u32 = 103;
pub const CXL_REJ_REASON: u32 = 102;
pub const CXL_REJ_RESPONSE_TO: u32 = 434;
pub const EXEC_INST: u32 = 18;

// --- MsgType values (Tag 35) ---
pub const MSG_HEARTBEAT: &[u8] = b"0";
pub const MSG_TEST_REQUEST: &[u8] = b"1";
pub const MSG_RESEND_REQUEST: &[u8] = b"2";
pub const MSG_REJECT: &[u8] = b"3";
pub const MSG_SEQUENCE_RESET: &[u8] = b"4";
pub const MSG_LOGOUT: &[u8] = b"5";
pub const MSG_LOGON: &[u8] = b"A";
pub const MSG_NEW_ORDER_SINGLE: &[u8] = b"D";
pub const MSG_ORDER_CANCEL_REQUEST: &[u8] = b"F";
pub const MSG_ORDER_CANCEL_REPLACE: &[u8] = b"G";
pub const MSG_EXECUTION_REPORT: &[u8] = b"8";
pub const MSG_ORDER_CANCEL_REJECT: &[u8] = b"9";

pub const FIX_4_2: &[u8] = b"FIX.4.2";

/// SOH delimiter (0x01) used to separate FIX fields.
pub const SOH: u8 = 0x01;
