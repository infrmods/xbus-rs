use ring::der::read_tag_and_get_value;
use untrusted::{Input, Reader};
use webpki::trust_anchor_util::cert_der_as_trust_anchor;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
enum Tag {
    Obid = 0x06,
    Seq = 0x20 | 0x10,
    Set = 0x20 | 0x11,
    Str = 0x13,
}

fn get_value<'a>(reader: &mut Reader<'a>, tag: Tag) -> Option<Input<'a>> {
    let (t, input) = read_tag_and_get_value(reader).ok()?;
    if t == tag as u8 {
        Some(input)
    } else {
        None
    }
}

const OBID_CN: [u8; 3] = [85, 4, 3];

pub fn get_cert_cn(cert_der: &[u8]) -> Option<String> {
    let anchor = cert_der_as_trust_anchor(Input::from(cert_der)).ok()?;
    let mut reader = Reader::new(Input::from(anchor.subject));
    while !reader.at_end() {
        let mut set = Reader::new(get_value(&mut reader, Tag::Set)?);
        let mut seq = Reader::new(get_value(&mut set, Tag::Seq)?);
        let obid = get_value(&mut seq, Tag::Obid)?;
        if obid.as_slice_less_safe() == OBID_CN.as_ref() {
            let data = get_value(&mut seq, Tag::Str)?;
            return Some(String::from_utf8_lossy(data.as_slice_less_safe()).into_owned());
        }
    }
    None
}
