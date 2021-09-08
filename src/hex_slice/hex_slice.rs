use std::fmt;

pub struct HexSlice<'a>(&'a [u8]);

impl<'a> HexSlice<'a> {
    fn new<T>(data: &'a T) -> HexSlice<'a>
    where
        T: ?Sized + AsRef<[u8]> + 'a,
    {
        HexSlice(data.as_ref())
    }
}

impl fmt::Display for HexSlice<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{:X}", byte)?;
        }
        Ok(())
    }
}

pub trait HexDisplayExt {
  fn hex_slice(&self) -> HexSlice<'_>;
}

impl<T> HexDisplayExt for T
where
  T: ?Sized + AsRef<[u8]>,
{
  fn hex_slice(&self) -> HexSlice<'_> {
      HexSlice::new(self)
  }
}