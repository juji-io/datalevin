//! Datalevin CSV records over UTF-16 units, independent of host stream objects.

pub enum Record {
    NeedInput,
    End,
    Row {
        cells: Vec<Vec<u16>>,
        consumed: usize,
        skip_lf: bool,
        finished: bool,
    },
}

/// Read one record. Incomplete records are retried when more input is available.
/// Quoting and backslash handling retain Datalevin's existing CSVReader format.
pub fn read_record(input: &[u16], eof: bool, separator: u16, quote: u16, skip_lf: bool) -> Record {
    let mut cells = Vec::new();
    let mut field = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut started = false;
    let mut skip_lf = skip_lf;
    for (position, &unit) in input.iter().enumerate() {
        if skip_lf {
            skip_lf = false;
            if unit == 10 {
                continue;
            }
        }
        started = true;
        if unit == separator {
            if quoted {
                if escaped {
                    field.push(92);
                    escaped = false;
                }
                field.push(unit);
            } else {
                cells.push(std::mem::take(&mut field));
            }
        } else if unit == quote {
            if quoted {
                if escaped {
                    field.push(unit);
                    escaped = false;
                } else {
                    quoted = false;
                }
            } else if field.is_empty() {
                quoted = true;
            } else {
                field.push(unit);
            }
        } else if unit == 10 || unit == 13 {
            if quoted {
                if escaped {
                    field.push(92);
                    escaped = false;
                }
                field.push(unit);
            } else {
                cells.push(field);
                return Record::Row {
                    cells,
                    consumed: position + 1,
                    skip_lf: unit == 13,
                    finished: false,
                };
            }
        } else if unit == 92 && quoted {
            if escaped {
                field.push(unit);
                escaped = false;
            } else {
                escaped = true;
            }
        } else {
            if escaped {
                field.push(92);
            }
            field.push(unit);
            escaped = false;
        }
    }
    if !eof {
        return Record::NeedInput;
    }
    if !started && cells.is_empty() && field.is_empty() {
        return Record::End;
    }
    if escaped {
        field.push(92);
    }
    cells.push(field);
    Record::Row {
        cells,
        consumed: input.len(),
        skip_lf,
        finished: true,
    }
}

/// Return whether to quote a cell and its escaped UTF-16 body. Host callbacks
/// may override the default quoting predicate without performing the escaping.
pub fn write_cell(input: &[u16], separator: u16, quote: u16, quote_override: Option<bool>) -> (bool, Vec<u16>) {
    let quoted = quote_override.unwrap_or_else(|| {
        input.iter().any(|&unit| unit == separator || unit == quote || unit == 10 || unit == 13)
    });
    let mut output = Vec::with_capacity(input.len());
    for &unit in input {
        if quoted && unit == quote {
            output.push(quote);
        }
        output.push(unit);
    }
    (quoted, output)
}
