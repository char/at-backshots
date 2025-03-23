// s32 from rsky-common thank u rudy <3

const TID_LEN: usize = 13;
const S32_CHAR: &str = "234567abcdefghijklmnopqrstuvwxyz";

#[rustfmt::skip]
const fn _s32_map() -> [u8; 256] {
    let mut map = [255u8; 256];
    let mut i = 0u8;

    macro_rules! chr {
        ($c: expr) => {
            map[$c as usize] = i; i += 1;
        };
    }

    chr!(b'2'); chr!(b'3'); chr!(b'4'); chr!(b'5'); chr!(b'6'); chr!(b'7');
    chr!(b'a'); chr!(b'b'); chr!(b'c'); chr!(b'd'); chr!(b'e'); chr!(b'f');
    chr!(b'g'); chr!(b'h'); chr!(b'i'); chr!(b'j'); chr!(b'k'); chr!(b'l');
    chr!(b'm'); chr!(b'n'); chr!(b'o'); chr!(b'p'); chr!(b'q'); chr!(b'r');
    chr!(b's'); chr!(b't'); chr!(b'u'); chr!(b'v'); chr!(b'w'); chr!(b'x');
    chr!(b'y'); chr!(b'z'); let _ = i;

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
