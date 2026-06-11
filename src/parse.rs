//! Lexer + parser for a small shell language.
//!
//! Phase 1 supports:
//!   * single + double quoted strings (`'...'`, `"..."`)
//!   * variable substitution: `$NAME`, `${NAME}` (in barewords and inside `"..."`)
//!   * pipelines: `a | b | c`
//!   * redirections: `> file`, `>> file`, `< file`, `2> file` (and other fd-prefixed forms)
//!   * sequencing: `&&`, `||`, `;`
//!   * comments: `# ...` to end of line
//!   * backslash escapes (outside single quotes)
//!
//! Out of scope for now: command substitution `$(...)`, glob, background `&`,
//! control flow, functions, heredocs, arrays, arithmetic.

use anyhow::{Result, anyhow};

// ---------- AST ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Stmt {
    Cmd(Command),
    Pipe(Vec<Command>),
    AndIf(Box<Stmt>, Box<Stmt>),
    OrIf(Box<Stmt>, Box<Stmt>),
    Seq(Box<Stmt>, Box<Stmt>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    pub words: Vec<Word>,
    pub redirects: Vec<Redirect>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    pub segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Segment {
    Literal(String),
    Var(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Redirect {
    pub fd: i32,
    pub op: RedirOp,
    pub target: Word,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RedirOp {
    In,
    Out,
    Append,
}

// ---------- Tokens ------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(Word),
    Pipe,       // |
    AndIf,      // &&
    OrIf,       // ||
    Semi,       // ;
    Less,       // <
    Great,      // >
    GreatGreat, // >>
    IoNumber(i32),
}

// ---------- Lexer -------------------------------------------------------

struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            src: s.as_bytes(),
            pos: 0,
        }
    }

    fn tokenize(mut self) -> Result<Vec<Tok>> {
        let mut out = Vec::new();
        loop {
            self.skip_ws_and_comments();
            let Some(c) = self.peek() else { break };
            match c {
                b'|' => {
                    self.pos += 1;
                    if self.peek() == Some(b'|') {
                        self.pos += 1;
                        out.push(Tok::OrIf);
                    } else {
                        out.push(Tok::Pipe);
                    }
                }
                b'&' => {
                    self.pos += 1;
                    if self.peek() == Some(b'&') {
                        self.pos += 1;
                        out.push(Tok::AndIf);
                    } else {
                        return Err(anyhow!("background '&' not supported yet"));
                    }
                }
                b';' => {
                    self.pos += 1;
                    out.push(Tok::Semi);
                }
                b'<' => {
                    self.pos += 1;
                    out.push(Tok::Less);
                }
                b'>' => {
                    self.pos += 1;
                    if self.peek() == Some(b'>') {
                        self.pos += 1;
                        out.push(Tok::GreatGreat);
                    } else {
                        out.push(Tok::Great);
                    }
                }
                c if c.is_ascii_digit() => {
                    // Could be IoNumber (digits immediately followed by < or >) or a word.
                    let start = self.pos;
                    while let Some(d) = self.peek() {
                        if d.is_ascii_digit() {
                            self.pos += 1;
                        } else {
                            break;
                        }
                    }
                    if matches!(self.peek(), Some(b'<') | Some(b'>')) {
                        let n: i32 = std::str::from_utf8(&self.src[start..self.pos])?
                            .parse()
                            .map_err(|e| anyhow!("bad io number: {e}"))?;
                        out.push(Tok::IoNumber(n));
                    } else {
                        self.pos = start;
                        out.push(Tok::Word(self.read_word()?));
                    }
                }
                _ => out.push(Tok::Word(self.read_word()?)),
            }
        }
        Ok(out)
    }

    fn read_word(&mut self) -> Result<Word> {
        let mut segments: Vec<Segment> = Vec::new();
        let mut lit = String::new();

        let flush = |lit: &mut String, segs: &mut Vec<Segment>| {
            if !lit.is_empty() {
                segs.push(Segment::Literal(std::mem::take(lit)));
            }
        };

        while let Some(c) = self.peek() {
            if matches!(
                c,
                b' ' | b'\t' | b'\n' | b'|' | b'&' | b';' | b'<' | b'>' | b'#'
            ) {
                break;
            }

            match c {
                b'\'' => {
                    self.pos += 1;
                    while let Some(d) = self.peek() {
                        if d == b'\'' {
                            break;
                        }
                        lit.push(d as char);
                        self.pos += 1;
                    }
                    if self.peek() != Some(b'\'') {
                        return Err(anyhow!("unterminated single quote"));
                    }
                    self.pos += 1;
                }
                b'"' => {
                    self.pos += 1;
                    while let Some(d) = self.peek() {
                        if d == b'"' {
                            break;
                        }
                        if d == b'$' {
                            flush(&mut lit, &mut segments);
                            let name = self.read_var_ref()?;
                            segments.push(Segment::Var(name));
                        } else if d == b'\\' {
                            self.pos += 1;
                            if let Some(esc) = self.peek() {
                                if matches!(esc, b'$' | b'`' | b'"' | b'\\' | b'\n') {
                                    lit.push(esc as char);
                                } else {
                                    lit.push('\\');
                                    lit.push(esc as char);
                                }
                                self.pos += 1;
                            }
                        } else {
                            lit.push(d as char);
                            self.pos += 1;
                        }
                    }
                    if self.peek() != Some(b'"') {
                        return Err(anyhow!("unterminated double quote"));
                    }
                    self.pos += 1;
                }
                b'$' => {
                    flush(&mut lit, &mut segments);
                    let name = self.read_var_ref()?;
                    segments.push(Segment::Var(name));
                }
                b'\\' => {
                    self.pos += 1;
                    if let Some(esc) = self.peek() {
                        lit.push(esc as char);
                        self.pos += 1;
                    }
                }
                _ => {
                    lit.push(c as char);
                    self.pos += 1;
                }
            }
        }

        if !lit.is_empty() {
            segments.push(Segment::Literal(lit));
        }
        Ok(Word { segments })
    }

    fn read_var_ref(&mut self) -> Result<String> {
        debug_assert_eq!(self.peek(), Some(b'$'));
        self.pos += 1;
        match self.peek() {
            Some(b'{') => {
                self.pos += 1;
                let start = self.pos;
                while let Some(d) = self.peek() {
                    if d == b'}' {
                        break;
                    }
                    self.pos += 1;
                }
                if self.peek() != Some(b'}') {
                    return Err(anyhow!("unterminated ${{"));
                }
                let name = std::str::from_utf8(&self.src[start..self.pos])?.to_string();
                self.pos += 1;
                if name.is_empty() {
                    return Err(anyhow!("empty variable name"));
                }
                Ok(name)
            }
            Some(c) if c.is_ascii_alphabetic() || c == b'_' => {
                let start = self.pos;
                while let Some(d) = self.peek() {
                    if d.is_ascii_alphanumeric() || d == b'_' {
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                Ok(std::str::from_utf8(&self.src[start..self.pos])?.to_string())
            }
            _ => Err(anyhow!("`$` must be followed by a name or `{{NAME}}`")),
        }
    }

    fn skip_ws_and_comments(&mut self) {
        loop {
            while let Some(c) = self.peek() {
                if c == b' ' || c == b'\t' || c == b'\n' {
                    self.pos += 1;
                } else {
                    break;
                }
            }
            if self.peek() == Some(b'#') {
                while let Some(c) = self.peek() {
                    if c == b'\n' {
                        break;
                    }
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }
}

// ---------- Parser ------------------------------------------------------

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn new(toks: Vec<Tok>) -> Self {
        Self { toks, pos: 0 }
    }

    fn parse(mut self) -> Result<Option<Stmt>> {
        if self.peek().is_none() {
            return Ok(None);
        }
        let s = self.parse_seq()?;
        if self.peek().is_some() {
            return Err(anyhow!("unexpected trailing tokens"));
        }
        Ok(Some(s))
    }

    fn parse_seq(&mut self) -> Result<Stmt> {
        let mut left = self.parse_and_or()?;
        while matches!(self.peek(), Some(Tok::Semi)) {
            self.advance();
            if self.peek().is_none() {
                return Ok(left);
            }
            let right = self.parse_and_or()?;
            left = Stmt::Seq(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and_or(&mut self) -> Result<Stmt> {
        let mut left = self.parse_pipeline()?;
        loop {
            match self.peek() {
                Some(Tok::AndIf) => {
                    self.advance();
                    let right = self.parse_pipeline()?;
                    left = Stmt::AndIf(Box::new(left), Box::new(right));
                }
                Some(Tok::OrIf) => {
                    self.advance();
                    let right = self.parse_pipeline()?;
                    left = Stmt::OrIf(Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_pipeline(&mut self) -> Result<Stmt> {
        let first = self.parse_command()?;
        if !matches!(self.peek(), Some(Tok::Pipe)) {
            return Ok(Stmt::Cmd(first));
        }
        let mut cmds = vec![first];
        while matches!(self.peek(), Some(Tok::Pipe)) {
            self.advance();
            cmds.push(self.parse_command()?);
        }
        Ok(Stmt::Pipe(cmds))
    }

    fn parse_command(&mut self) -> Result<Command> {
        let mut words = Vec::new();
        let mut redirects = Vec::new();

        loop {
            if let Some(Tok::IoNumber(_)) = self.peek() {
                let Tok::IoNumber(fd) = self.advance() else {
                    unreachable!()
                };
                let op = match self.advance_or("expected < or > after fd number")? {
                    Tok::Less => RedirOp::In,
                    Tok::Great => RedirOp::Out,
                    Tok::GreatGreat => RedirOp::Append,
                    _ => return Err(anyhow!("expected < or > after fd number")),
                };
                let target = match self.advance_or("expected file name after redirect")? {
                    Tok::Word(w) => w,
                    _ => return Err(anyhow!("expected file name after redirect")),
                };
                redirects.push(Redirect { fd, op, target });
                continue;
            }
            match self.peek() {
                Some(Tok::Word(_)) => {
                    let Tok::Word(w) = self.advance() else {
                        unreachable!()
                    };
                    words.push(w);
                }
                Some(Tok::Less) => {
                    self.advance();
                    let target = match self.advance_or("expected file name after <")? {
                        Tok::Word(w) => w,
                        _ => return Err(anyhow!("expected file name after <")),
                    };
                    redirects.push(Redirect {
                        fd: 0,
                        op: RedirOp::In,
                        target,
                    });
                }
                Some(Tok::Great) => {
                    self.advance();
                    let target = match self.advance_or("expected file name after >")? {
                        Tok::Word(w) => w,
                        _ => return Err(anyhow!("expected file name after >")),
                    };
                    redirects.push(Redirect {
                        fd: 1,
                        op: RedirOp::Out,
                        target,
                    });
                }
                Some(Tok::GreatGreat) => {
                    self.advance();
                    let target = match self.advance_or("expected file name after >>")? {
                        Tok::Word(w) => w,
                        _ => return Err(anyhow!("expected file name after >>")),
                    };
                    redirects.push(Redirect {
                        fd: 1,
                        op: RedirOp::Append,
                        target,
                    });
                }
                _ => break,
            }
        }
        if words.is_empty() && redirects.is_empty() {
            return Err(anyhow!("empty command"));
        }
        Ok(Command { words, redirects })
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn advance(&mut self) -> Tok {
        let t = self.toks[self.pos].clone();
        self.pos += 1;
        t
    }

    fn advance_or(&mut self, msg: &'static str) -> Result<Tok> {
        if self.peek().is_none() {
            return Err(anyhow!(msg));
        }
        Ok(self.advance())
    }
}

/// Parse one command line into a Stmt (or None for empty input).
pub fn parse(source: &str) -> Result<Option<Stmt>> {
    let toks = Lexer::new(source).tokenize()?;
    Parser::new(toks).parse()
}

// ---------- Tests --------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn w(s: &str) -> Word {
        Word {
            segments: vec![Segment::Literal(s.into())],
        }
    }

    fn cmd_words(stmt: &Stmt) -> Vec<Word> {
        match stmt {
            Stmt::Cmd(c) => c.words.clone(),
            _ => panic!("not a cmd"),
        }
    }

    #[test]
    fn bare_words() {
        let s = parse("ls -la foo").unwrap().unwrap();
        assert_eq!(cmd_words(&s), vec![w("ls"), w("-la"), w("foo")]);
    }

    #[test]
    fn single_quoted() {
        let s = parse("echo 'hello world'").unwrap().unwrap();
        assert_eq!(cmd_words(&s), vec![w("echo"), w("hello world")]);
    }

    #[test]
    fn double_quoted_with_var() {
        let s = parse(r#"echo "hi $USER""#).unwrap().unwrap();
        let words = cmd_words(&s);
        assert_eq!(words[0], w("echo"));
        assert_eq!(
            words[1],
            Word {
                segments: vec![Segment::Literal("hi ".into()), Segment::Var("USER".into()),]
            }
        );
    }

    #[test]
    fn bareword_var() {
        let s = parse("echo $HOME/x").unwrap().unwrap();
        let words = cmd_words(&s);
        assert_eq!(
            words[1],
            Word {
                segments: vec![Segment::Var("HOME".into()), Segment::Literal("/x".into())]
            }
        );
    }

    #[test]
    fn braced_var() {
        let s = parse("echo ${FOO}bar").unwrap().unwrap();
        let words = cmd_words(&s);
        assert_eq!(
            words[1],
            Word {
                segments: vec![Segment::Var("FOO".into()), Segment::Literal("bar".into())]
            }
        );
    }

    #[test]
    fn pipeline() {
        let s = parse("a | b | c").unwrap().unwrap();
        match s {
            Stmt::Pipe(cmds) => assert_eq!(cmds.len(), 3),
            _ => panic!("expected pipe"),
        }
    }

    #[test]
    fn and_or_chain() {
        let s = parse("a && b || c").unwrap().unwrap();
        match s {
            Stmt::OrIf(l, _) => match *l {
                Stmt::AndIf(_, _) => {}
                _ => panic!("expected and inside or"),
            },
            _ => panic!("expected or"),
        }
    }

    #[test]
    fn sequence() {
        let s = parse("a; b; c").unwrap().unwrap();
        match s {
            Stmt::Seq(_, _) => {}
            _ => panic!("expected seq"),
        }
    }

    #[test]
    fn redirect_out() {
        let s = parse("ls > out.txt").unwrap().unwrap();
        match s {
            Stmt::Cmd(c) => {
                assert_eq!(c.redirects.len(), 1);
                assert_eq!(c.redirects[0].fd, 1);
                assert_eq!(c.redirects[0].op, RedirOp::Out);
                assert_eq!(c.redirects[0].target, w("out.txt"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn redirect_stderr() {
        let s = parse("foo 2> err").unwrap().unwrap();
        match s {
            Stmt::Cmd(c) => {
                assert_eq!(c.redirects[0].fd, 2);
                assert_eq!(c.redirects[0].op, RedirOp::Out);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn redirect_append() {
        let s = parse("foo >> log").unwrap().unwrap();
        match s {
            Stmt::Cmd(c) => {
                assert_eq!(c.redirects[0].op, RedirOp::Append);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn comment_ignored() {
        let s = parse("ls # this is a comment").unwrap().unwrap();
        assert_eq!(cmd_words(&s), vec![w("ls")]);
    }

    #[test]
    fn empty_input() {
        assert!(parse("").unwrap().is_none());
        assert!(parse("   # only comment").unwrap().is_none());
    }

    #[test]
    fn errors_on_unterminated_quote() {
        assert!(parse(r#"echo "unterminated"#).is_err());
        assert!(parse("echo 'unterminated").is_err());
    }
}
