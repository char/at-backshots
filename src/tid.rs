// s32 from rsky-common thank u rudy <3

const TID_LEN: usize = 13;
const S32_CHAR: &str = "234567abcdefghijklmnopqrstuvwxyz";

#[rustfmt::skip]
const fn _s32_map() -> [u8; 256] {
    let mut map = [255u8; 256];
    let mut i = 0u8;
    let charset = b"234567abcdefghijklmnopqrstuvwxyz";
    while i < 32 {
        map[charset[i as usize] as usize] = i;
        i += 1;
    }
    map
}
const S32_MAP: [u8; 256] = _s32_map();

pub fn s32encode(mut i: u64) -> String {
    let mut s: String = "".to_owned();
    while i > 0 {
        let c = i % 32;
        i /= 32;
        s = format!("{0}{1}", S32_CHAR.chars().nth(c as usize).unwrap(), s);
    }
    s
}

pub fn s32decode(s: &str) -> u64 {
    let mut i: u64 = 0;
    for c in s.bytes() {
        i = i * 32 + S32_MAP[c as usize] as u64;
    }
    i
}

pub fn is_tid(s: &str) -> bool {
    s.len() == TID_LEN && s.bytes().all(|it| S32_MAP[it as usize] != 255)
}

#[test]
fn test() {
    assert_eq!(s32decode(&s32encode(1337)), 1337);
    assert!(is_tid("3lkzfmkh3es2l"));
}
