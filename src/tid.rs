// s32 from rsky-common thank u rudy <3

const TID_LEN: usize = 13;
const S32_CHAR: &str = "234567abcdefghijklmnopqrstuvwxyz";

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
    for c in s.chars() {
        i = i * 32 + S32_CHAR.chars().position(|x| x == c).unwrap() as u64;
    }
    i
}

pub fn is_tid(s: &str) -> bool {
    s.len() == TID_LEN && s.chars().all(|it| S32_CHAR.contains(it))
}
