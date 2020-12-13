package fluxql

import (
	"context"
	"io"
	"log"
	"strings"
)

type Parser struct{}

func (p *Parser) Parse(ctx context.Context, rs io.RuneScanner) error {
	scn := &Scanner{}
	tokens := []Token{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lookahead := 1

	tch := scn.Scan(ctx, rs)

	match := func(toks ...TokenType) bool {
		start, end := func() int {
			if len(tokens) > lookahead {
				return len(tokens) - (len(toks) + 1), len(tokens) - (lookahead + 1)
			}
			return len(tokens) - (len(toks) + 1), len(tokens) - 1
		}()

	}

	for {
		token, ok := <-tch
		if !ok && len(tokens) == 0 {
			break
		}

		tokens = append(tokens, token)

		if len(tokens) <= lookahead {
			log.Printf("filling lookahead of %d", lookahead)
			continue
		}

		log.Printf("%v", tokens)

		// get handle on last entry in our token list
		end := len(tokens) - 1
		log.Printf("END: %v", tokens[end])
		switch {

		case len(tokens) > 2 && tokens[end-2].Type == TokenColumnList && tokens[end-1].Type == TokenType(',') && tokens[end].Type == TokenColumnName:
			cl := tokens[end-2].Value.([]string)
			cl = append(cl, tokens[end].Value.(string))
			tokens = tokens[:len(tokens)-3]

			tokens = append(tokens, Token{TokenColumnList, cl})

			tokens[end].Type = TokenColumnList
			switch tokens[end].Value.(type) {
			case TokenType:
				tokens[end].Value = []string{"*"}
			case string:
				tokens[end].Value = []string{tokens[end].Value.(string)}
			}

		case len(tokens) > 1 && tokens[end-1].Type == TokenSelect && tokens[end].Type == TokenIdentifier:

		case len(tokens) > 2 && tokens[end-2].Type == TokenIdentifier && tokens[end-1].Type == TokenType(',') && (tokens[end].Type == TokenIdentifier || tokens[end].Type == TokenWildCard):
			// identifier '.' identifier
			id := tokens[end-2].Value.(string) + "." + tokens[end].Value.(string)
			tokens = tokens[:len(tokens)-3]
			tokens = append(tokens, Token{Type: TokenColumnName, Value: id})

		case len(tokens) > 0 && tokens[end].Type == TokenWildCard:
			id := string(tokens[end].Value.(rune))
			tokens = tokens[:len(tokens)-1]
			tokens = append(tokens, Token{Type: TokenColumnName, Value: id})

		case len(tokens) > 0 && tokens[end].Type == TokenType('*'):
			tokens[end].Value = "*"
			tokens[end].Type = TokenWildCard

		case len(tokens) > 0 && tokens[end].Type == TokenSymbol:
			tokens[end].Type = TokenIdentifier

		case len(tokens) > 0 && tokens[end].Type == TokenIdentifier:
			// lets see if this is a keyword

			kwmap := map[string]TokenType{
				"select": TokenSelect,
				"update": TokenUpdate,
				"insert": TokenInsert,
				"from":   TokenFrom,
				"where":  TokenWhere,
				"join":   TokenJoin,
				"outer":  TokenOuter,
				"inner":  TokenInner,
				"with":   TokenWith,
			}

			if tt, exists := kwmap[strings.ToLower(tokens[end].Value.(string))]; exists {
				tokens[end].Type = tt
			}
		}
	}
	return nil
}
