package fluxql

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"unicode"
)

type Token struct {
	Type  TokenType
	Value interface{}
}

const (
	TokenAtom = TokenType(iota + 0xf0000)
	TokenColumnList
	TokenFloat
	TokenFrom
	TokenInner
	TokenInsert
	TokenInteger
	TokenJoin
	TokenOuter
	TokenQuotedString
	TokenSelect
	TokenSymbol
	TokenUpdate
	TokenWhere
	TokenIdentifier
	TokenWith
	TokenColumnName
	TokenWildCard
)

type TokenType rune

func (t Token) String() string {
	m := map[TokenType]string{
		TokenAtom:         "TokenAtom",
		TokenWildCard:     "TokenWildCard",
		TokenColumnList:   "TokenColumnList",
		TokenFloat:        "TokenFloat",
		TokenFrom:         "TokenFrom",
		TokenInner:        "TokenInner",
		TokenInsert:       "TokenInsert",
		TokenInteger:      "TokenInteger",
		TokenJoin:         "TokenJoin",
		TokenOuter:        "TokenOuter",
		TokenQuotedString: "TokenQuotedString",
		TokenSelect:       "TokenSelect",
		TokenSymbol:       "TokenSymbol",
		TokenIdentifier:   "TokenIdentifier",
		TokenUpdate:       "TokenUpdate",
		TokenWhere:        "TokenWhere",
		TokenWith:         "TokenWith",
		TokenColumnName:   "TokenColumnName",
	}

	s, exists := m[t.Type]
	if !exists {
		return fmt.Sprintf("TokenRune(%c)", t.Type)
	}
	return fmt.Sprintf("%s(%q)", s, t.Value)
}

type Scanner struct {
}

func (s *Scanner) match(rs io.RuneScanner, match func(rune) (bool, bool, error)) string {
	sb := &strings.Builder{}

mainloop:
	for {
		r, _, err := rs.ReadRune()

		accept, cont, err := match(r)

		if err != nil {
			rs.UnreadRune()
			break mainloop
		}

		if accept {
			sb.WriteRune(r)
		}

		if !cont {
			break mainloop
		}
	}

	return sb.String()
}

func (s *Scanner) scan(ctx context.Context, rs io.RuneScanner, ch chan<- Token) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// peek next rune
		r, _, err := rs.ReadRune()
		if err != nil {
			return err
		}
		rs.UnreadRune()

		// classify rune

		switch {
		case unicode.IsSpace(r):
			// if we have a space, lets match all space characters and ignore them
			s.match(rs, func(r rune) (bool, bool, error) {
				if !unicode.IsSpace(r) {
					return false, false, fmt.Errorf("not white space")
				}
				return true, true, nil
			})

		case r == '"' || r == '\'':
			// quoted string
			qtype := r
			count := 0
			escaped := false

			str := s.match(rs, func(r rune) (bool, bool, error) {
				count++
				if escaped == true {
					escaped = false
					return true, true, nil
				}

				if r == qtype {
					if count == 1 {
						return false, true, nil // dont't accept, continue, no error
					}

					if count != 1 {
						return false, false, nil // don't accept, don't continue, no error
					}
				}

				if r == '\\' {
					escaped = true
					return false, true, nil // don't accept, continue, no error
				}

				return true, true, nil // accept, continue, no error
			})
			ch <- Token{Type: TokenQuotedString, Value: str}

		case unicode.IsDigit(r):
			// either an integer or float

			tokenType := TokenInteger // type of token being read.  if we determine this is a float, the match function will set this to TokenFloat
			decimals := 0             // number of decial points read.  should be at most 1
			digits := 0               // number of digits scanned so far

			number := s.match(rs, func(r rune) (bool, bool, error) {
				digits++
				// check for leading '+' or ' -'
				if r == '+' || r == '-' {
					if digits != 1 {
						return false, false, fmt.Errorf("unexpected %v character", r)
					}
					return true, true, nil
				}

				if r == '.' {
					decimals++
					tokenType = TokenFloat

					if decimals > 1 {
						return false, false, fmt.Errorf("unexpected %v character", r)
					}

					return true, true, nil
				}

				if decimals > 1 {
					return false, false, fmt.Errorf("too many decimals in number")
				}

				if unicode.IsDigit(r) {
					return true, true, nil
				}

				return false, false, fmt.Errorf("not a digit character")

			})
			ch <- Token{Type: tokenType, Value: number}
		case unicode.IsLetter(r):
			count := 0
			atom := s.match(rs, func(r rune) (bool, bool, error) {
				count++
				if (count == 1 && unicode.IsLetter(r)) || (count > 1 && (unicode.IsLetter(r) || (unicode.IsDigit(r)))) {
					return true, true, nil
				}
				return false, false, fmt.Errorf("not a letter")
			})
			ch <- Token{Type: TokenIdentifier, Value: atom}
		default:
			rs.ReadRune()
			ch <- Token{Type: TokenType(r), Value: r}
		}
	}

	return nil
}

func (s *Scanner) Scan(ctx context.Context, rs io.RuneScanner) <-chan Token {
	ch := make(chan Token)

	go func() {
		if err := s.scan(ctx, rs, ch); err != nil {
			log.Printf("scanner error: %v", err)
		}
		close(ch)
	}()

	return ch
}
