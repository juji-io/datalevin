//! Ordered byte-string keys using Datalevin's persisted Hu-Tucker alphabet.

pub const SYMBOL_COUNT: usize = 65_793;

#[derive(Debug)]
pub enum DictionaryError {
    Size(usize, usize),
    Code { symbol: usize, length: u8, code: u32 },
    Overlap(usize),
    Incomplete,
    NotAlphabetic,
    Truncated,
    Magic([u8; 4]),
    Version(u8),
    Flags(u8, u16),
    HeaderSize(u8, i32, i32),
}

#[derive(Debug)]
pub enum KeyError {
    MissingTerminator,
    TrailingData,
}

struct Node {
    children: [Option<usize>; 2],
    symbol: Option<usize>,
}

impl Node {
    fn branch() -> Self {
        Self { children: [None; 2], symbol: None }
    }
}

pub struct Dictionary {
    lengths: Vec<u8>,
    codes: Vec<u32>,
    nodes: Vec<Node>,
}

/// A group is emitted after consuming `consumed` source bytes. Bindings replay
/// each byte separately so a short or read-only output keeps buffer progress.
pub struct Write {
    pub consumed: usize,
    pub bytes: Vec<u8>,
}

pub struct Output {
    pub writes: Vec<Write>,
    pub consumed: usize,
    pub padded: bool,
    pub error: Option<KeyError>,
}

impl Dictionary {
    pub fn new(lengths: Vec<u8>, codes: Vec<u32>) -> Result<Self, DictionaryError> {
        if lengths.len() != SYMBOL_COUNT || codes.len() != SYMBOL_COUNT {
            return Err(DictionaryError::Size(lengths.len(), codes.len()));
        }
        let mut nodes = vec![Node::branch()];
        for (symbol, (&length, &code)) in lengths.iter().zip(&codes).enumerate() {
            if !(1..=32).contains(&length) || u64::from(code) >= (1_u64 << length) {
                return Err(DictionaryError::Code { symbol, length, code });
            }
            let mut node = 0;
            for bit in (0..length).rev() {
                let side = ((code >> bit) & 1) as usize;
                if let Some(child) = nodes[node].children[side] {
                    if bit == 0 || nodes[child].symbol.is_some() {
                        return Err(DictionaryError::Overlap(symbol));
                    }
                    node = child;
                } else {
                    let child = nodes.len();
                    nodes.push(Node::branch());
                    nodes[node].children[side] = Some(child);
                    node = child;
                }
            }
            nodes[node].symbol = Some(symbol);
        }
        let mut expected = 0;
        let mut stack = vec![0];
        while let Some(index) = stack.pop() {
            let node = &nodes[index];
            if let Some(symbol) = node.symbol {
                if symbol != expected {
                    return Err(DictionaryError::NotAlphabetic);
                }
                expected += 1;
            } else {
                let [Some(left), Some(right)] = node.children else {
                    return Err(DictionaryError::Incomplete);
                };
                stack.push(right);
                stack.push(left);
            }
        }
        Ok(Self { lengths, codes, nodes })
    }

    pub fn load(input: &[u8]) -> Result<Self, DictionaryError> {
        let magic: [u8; 4] = input.get(..4).ok_or(DictionaryError::Truncated)?.try_into().unwrap();
        if &magic != b"HUTU" {
            return Err(DictionaryError::Magic(magic));
        }
        let header = input.get(..16).ok_or(DictionaryError::Truncated)?;
        let version = header[4];
        let flags = header[5];
        let reserved = u16::from_be_bytes(header[6..8].try_into().unwrap());
        let lengths = i32::from_be_bytes(header[8..12].try_into().unwrap());
        let codes = i32::from_be_bytes(header[12..16].try_into().unwrap());
        if version != 1 {
            return Err(DictionaryError::Version(version));
        }
        if flags != 0 || reserved != 0 {
            return Err(DictionaryError::Flags(flags, reserved));
        }
        if lengths != SYMBOL_COUNT as i32 || codes != SYMBOL_COUNT as i32 {
            return Err(DictionaryError::HeaderSize(version, lengths, codes));
        }
        let payload = input.get(16..16 + 5 * SYMBOL_COUNT).ok_or(DictionaryError::Truncated)?;
        let lengths = payload[..SYMBOL_COUNT].to_vec();
        // The existing dictionary format stores its code words little-endian,
        // independently of the byte-oriented key encoding and buffer order.
        let codes = payload[SYMBOL_COUNT..].chunks_exact(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap())).collect();
        Self::new(lengths, codes)
    }

    pub fn dump(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(16 + 5 * SYMBOL_COUNT);
        output.extend(b"HUTU\x01\0\0\0");
        output.extend((SYMBOL_COUNT as u32).to_be_bytes());
        output.extend((SYMBOL_COUNT as u32).to_be_bytes());
        output.extend(&self.lengths);
        for code in &self.codes {
            output.extend(code.to_le_bytes());
        }
        output
    }

    pub fn encode(&self, input: &[u8]) -> Output {
        let mut output = Output { writes: Vec::new(), consumed: 0, padded: false, error: None };
        let mut byte = 0_u8;
        let mut remaining = 8;
        loop {
            let available = input.len() - output.consumed;
            let symbol = if available >= 2 {
                let pair = usize::from(u16::from_be_bytes(input[output.consumed..output.consumed + 2].try_into().unwrap()));
                output.consumed += 2;
                2 + pair + (pair >> 8)
            } else if available == 1 {
                output.consumed += 1;
                1 + 257 * usize::from(input[output.consumed - 1])
            } else { 0 };
            let mut bytes = Vec::new();
            for bit in (0..self.lengths[symbol]).rev() {
                remaining -= 1;
                byte |= (((self.codes[symbol] >> bit) & 1) as u8) << remaining;
                if remaining == 0 {
                    bytes.push(byte);
                    byte = 0;
                    remaining = 8;
                }
            }
            if available < 2 {
                output.padded = remaining < 8;
                if output.padded { bytes.push(byte); }
            }
            if !bytes.is_empty() {
                output.writes.push(Write { consumed: output.consumed, bytes });
            }
            if available < 2 { return output; }
        }
    }

    pub fn decode(&self, input: &[u8]) -> Output {
        let mut output = Output { writes: Vec::new(), consumed: 0, padded: false, error: None };
        let mut node = 0;
        for (index, &byte) in input.iter().enumerate() {
            output.consumed = index + 1;
            // Flush each nibble, as the reference decoder does. The trie itself
            // stays independent of its nibble lookup-table implementation.
            for high in [true, false] {
                let bits = if high { (4..8).rev() } else { (0..4).rev() };
                let mut bytes = Vec::new();
                let mut terminal = false;
                for bit in bits {
                    node = self.nodes[node].children[((byte >> bit) & 1) as usize].unwrap();
                    if let Some(symbol) = self.nodes[node].symbol {
                        node = 0;
                        terminal = symbol == 0 || (symbol - 1) % 257 == 0;
                        if terminal {
                            if symbol != 0 { bytes.push(((symbol - 1) / 257) as u8); }
                            if index + 1 != input.len() || byte & ((1_u16 << bit) - 1) as u8 != 0 {
                                output.error = Some(KeyError::TrailingData);
                            }
                            break;
                        }
                        let pair = symbol - 2;
                        bytes.extend([(pair / 257) as u8, (pair % 257) as u8]);
                    }
                }
                if !bytes.is_empty() { output.writes.push(Write { consumed: index + 1, bytes }); }
                if terminal { return output; }
            }
        }
        output.error = Some(KeyError::MissingTerminator);
        output
    }
}
