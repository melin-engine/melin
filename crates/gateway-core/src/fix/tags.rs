//! FIX 4.4 tag constants and message type codes.

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

// --- Market data tags (FIX 4.4) ---
pub const MD_REQ_ID: u32 = 262;
pub const SUBSCRIPTION_REQUEST_TYPE: u32 = 263;
pub const MARKET_DEPTH: u32 = 264;
pub const MD_UPDATE_TYPE: u32 = 265;
pub const NO_MD_ENTRIES: u32 = 268;
pub const MD_ENTRY_TYPE: u32 = 269;
pub const MD_ENTRY_PX: u32 = 270;
pub const MD_ENTRY_SIZE: u32 = 271;
pub const MD_UPDATE_ACTION: u32 = 279;
pub const NO_RELATED_SYM: u32 = 146;
pub const NUMBER_OF_ORDERS: u32 = 346;
pub const MD_REQ_REJ_REASON: u32 = 281;

// --- Security list tags (FIX 4.4) ---
pub const SECURITY_REQ_ID: u32 = 320;
pub const SECURITY_LIST_REQUEST_TYPE: u32 = 559;
pub const SECURITY_RESPONSE_ID: u32 = 322;
pub const SECURITY_REQUEST_RESULT: u32 = 560;
pub const MIN_PRICE_INCREMENT: u32 = 969;
pub const ROUND_LOT: u32 = 561;
pub const CURRENCY: u32 = 15;
pub const SETTL_CURRENCY: u32 = 120;

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
pub const MSG_MARKET_DATA_REQUEST: &[u8] = b"V";
pub const MSG_MD_SNAPSHOT: &[u8] = b"W";
pub const MSG_MD_INCREMENTAL: &[u8] = b"X";
pub const MSG_MD_REQUEST_REJECT: &[u8] = b"Y";
pub const MSG_SECURITY_LIST_REQUEST: &[u8] = b"x";
pub const MSG_SECURITY_LIST: &[u8] = b"y";

pub const FIX_VERSION: &[u8] = b"FIX.4.4";

/// SOH delimiter (0x01) used to separate FIX fields.
pub const SOH: u8 = 0x01;
