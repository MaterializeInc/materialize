// Shamelessly stolen from Aleph-Alpha/ts-rs.
// MIT License Copyright (c) 2020 Aleph Alpha GmbH

use std::{fmt::Display, io::Write};

use termcolor::{BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

// Sadly, it is impossible to raise a warning in a proc macro.
// This function prints a message which looks like a compiler warning.
pub fn print_warning(
    title: impl Display,
    content: impl Display,
    note: impl Display,
) -> std::io::Result<()> {
    let make_color = |color: Color, bold: bool| {
        let mut spec = ColorSpec::new();
        spec.set_fg(Some(color)).set_bold(bold).set_intense(true);
        spec
    };

    let yellow_bold = make_color(Color::Yellow, true);
    let white_bold = make_color(Color::White, true);
    let white = make_color(Color::White, false);
    let blue = make_color(Color::Blue, true);

    let writer = BufferWriter::stderr(ColorChoice::Auto);
    let mut buffer = writer.buffer();

    buffer.set_color(&yellow_bold)?;
    write!(&mut buffer, "warning")?;
    buffer.set_color(&white_bold)?;
    writeln!(&mut buffer, ": {}", title)?;

    buffer.set_color(&blue)?;
    writeln!(&mut buffer, "  | ")?;

    let content = format!("{content}");
    for line in content.split('\n') {
        write!(&mut buffer, "  | ")?;
        buffer.set_color(&white)?;
        writeln!(&mut buffer, "{}", line)?;
    }

    buffer.set_color(&blue)?;
    writeln!(&mut buffer, "  | ")?;

    write!(&mut buffer, "  = ")?;
    buffer.set_color(&white_bold)?;
    write!(&mut buffer, "note: ")?;
    buffer.set_color(&white)?;
    writeln!(&mut buffer, "{}", note)?;

    writer.print(&buffer)
}
