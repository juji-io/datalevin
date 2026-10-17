//! Dictionary-backed value compression for Datalevin's existing Zstandard format.

use zstd::zstd_safe::{CCtx, CDict, DCtx, DDict};

pub enum Error {
    Setup,
    Codec(usize),
}

pub fn valid_dictionary(dictionary: &[u8]) -> bool {
    CDict::try_create(dictionary, 3).is_some() && DDict::try_create(dictionary).is_some()
}

/// Compress into caller-provided storage, retaining writes even on codec errors.
pub fn compress(dictionary: &[u8], input: &[u8], output: &mut [u8]) -> Result<usize, Error> {
    let dictionary = CDict::try_create(dictionary, 3).ok_or(Error::Setup)?;
    let mut context = CCtx::try_create().ok_or(Error::Setup)?;
    context.compress_using_cdict(output, input, &dictionary).map_err(Error::Codec)
}

pub fn decompress(dictionary: &[u8], input: &[u8], output: &mut [u8]) -> Result<usize, Error> {
    let dictionary = DDict::try_create(dictionary).ok_or(Error::Setup)?;
    let mut context = DCtx::try_create().ok_or(Error::Setup)?;
    context.decompress_using_ddict(output, input, &dictionary).map_err(Error::Codec)
}

impl Error {
    pub fn code(&self) -> Option<i64> {
        match self {
            Self::Codec(code) => Some((*code as i64).wrapping_neg()),
            Self::Setup => None,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::Codec(code) => zstd::zstd_safe::get_error_name(*code),
            Self::Setup => "Zstandard dictionary or context creation failed",
        }
    }
}
