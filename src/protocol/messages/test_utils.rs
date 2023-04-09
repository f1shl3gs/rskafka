macro_rules! test_roundtrip_versioned {
    ($t:ty, $min:expr, $max:expr, $name:ident) => {
        #[allow(unused_imports)]
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig{fork: false, ..Default::default()})]
            #[test]
            fn $name(orig: $t) {
                #[allow(unused_imports)]
                use std::io::Cursor;

                for v in ($min.0)..=($max.0) {
                    let v = ApiVersion(v);

                    let mut buf = Cursor::new(Vec::<u8>::new());
                    match orig.write_versioned(&mut buf, v) {
                        Err(_) => {
                            // skip
                        }
                        Ok(()) => {
                            buf.set_position(0);
                            let restored_1 = <$t>::read_versioned(&mut buf, v).unwrap();

                            // `orig` and `restored` might be different here, so we need another roundtrip
                            let mut buf = Cursor::new(Vec::<u8>::new());
                            restored_1.write_versioned(&mut buf, v).unwrap();

                            let l = buf.position();
                            buf.set_position(0);

                            let restored_2 = <$t>::read_versioned(&mut buf, v).unwrap();
                            assert_eq!(restored_1, restored_2);

                            assert_eq!(buf.position(), l);
                        }
                    }
                }
            }
        }
    };
}

pub(crate) use test_roundtrip_versioned;

macro_rules! assert_write_versioned {
    ($req:expr, $version:expr, $want: expr) => {
        let mut buf = Vec::new();
        $req.write_versioned(&mut buf, ApiVersion::new($version))
            .unwrap();
        assert_eq!(buf, $want);
    };
}

pub(crate) use assert_write_versioned;

macro_rules! assert_read_versioned {
    ($input:expr, $version:expr, $want:expr) => {
        let mut reader = std::io::Cursor::new($input);
        let got = OffsetFetchResponse::read_versioned(&mut reader, ApiVersion::new(0)).unwrap();
        assert_eq!($want, got);
    };
}

pub(crate) use assert_read_versioned;
