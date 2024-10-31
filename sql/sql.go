package sql

import (
	"strings"
)

type Select struct {
	Column    string
	Table     string
	Alias     string
	Aggregate string
}

type GeneratorOptions struct {
	CaseSensitive bool
	ForceGroupBy  bool
}

type SQLGenerator struct {
	DB       *Database
	Options  GeneratorOptions
	FromName string
	Selects  []Select
}

func (gen *SQLGenerator) AddSelect(s Select) {
	if gen.FromName == "" {
		gen.FromName = s.Table
	}
	gen.Selects = append(gen.Selects, s)
}

type SurroundWriter interface {
	WriteOpen(b *strings.Builder)
	WriteClose(b *strings.Builder)
}

func WriteSurrounded(b *strings.Builder, sw SurroundWriter, content Writer) {
	sw.WriteOpen(b)
	content.Write(b)
	sw.WriteClose(b)
}

type FnCallWriter struct {
	fnName string
}

func (aw FnCallWriter) WriteOpen(b *strings.Builder) {
	b.WriteString(aw.fnName)
	b.WriteString("(")
}

func (aw FnCallWriter) WriteClose(b *strings.Builder) {
	b.WriteString(")")
}

type SurroundSimple struct {
	Open  string
	Close string
}

func (s SurroundSimple) WriteOpen(b *strings.Builder) {
	b.WriteString(s.Open)
}

func (s SurroundSimple) WriteClose(b *strings.Builder) {
	b.WriteString(s.Close)
}

type SurroundNone struct {
}

func (s SurroundNone) WriteOpen(b *strings.Builder) {
}

func (s SurroundNone) WriteClose(b *strings.Builder) {
}

type TextWriter struct {
	Text string
}

func (w TextWriter) Write(b *strings.Builder) {
	b.WriteString(w.Text)
}

type IdentifierWriter struct {
	Quote      bool
	Identifier string
	Namespace  string
}

func QuoteSurroundWriter(useQuote bool) SurroundWriter {
	if useQuote {
		return SurroundSimple{"\"", "\""}
	} else {
		return SurroundNone{}
	}
}

func AggregateSurroundWriter(aggregate string) SurroundWriter {
	if aggregate == "" {
		return SurroundNone{}
	} else {
		return FnCallWriter{fnName: aggregate}
	}
}

func (iw IdentifierWriter) Write(b *strings.Builder) {
	var sw = QuoteSurroundWriter(iw.Quote)
	if iw.Namespace != "" {
		WriteSurrounded(b, sw, TextWriter{iw.Namespace})
		b.WriteString(".")
	}
	WriteSurrounded(b, sw, TextWriter{iw.Identifier})
}

type SelectWriter struct {
	Select
	GeneratorOptions
}

func (s SelectWriter) Write(b *strings.Builder) {
	WriteSurrounded(b, AggregateSurroundWriter(s.Aggregate), IdentifierWriter{
		Quote:      s.CaseSensitive,
		Identifier: s.Column,
		Namespace:  s.Table,
	})
	if s.Alias != "" {
		b.WriteString(" as ")
		WriteSurrounded(b, QuoteSurroundWriter(true), TextWriter{
			Text: s.Alias,
		})
	}
}

type JoinWriter struct {
	*SQLGenerator
}

func (jw JoinWriter) Write(b *strings.Builder) {
	jw.IterateJoins(func(join SQLJoin) {
		join.Write(b)
	})
}

type SQLJoin struct {
	JoinTarget Writer
	OnCond     Writer
	Equals     Writer
}

func (join SQLJoin) Write(b *strings.Builder) {
	b.WriteString("\n")
	b.WriteString("join ")
	join.JoinTarget.Write(b)
	b.WriteString(" on ")
	join.OnCond.Write(b)
	b.WriteString(" = ")
	join.Equals.Write(b)
}

func (jw JoinWriter) IterateJoins(fn func(join SQLJoin)) {
	gen := jw.SQLGenerator
	fkeys := gen.DB.GetTable(gen.FromName).FKeys
	fkeys.Iter(func(fkey ForeignKey) bool {
		var join SQLJoin
		join.JoinTarget = IdentifierWriter{
			Quote:      gen.Options.CaseSensitive,
			Identifier: fkey.Target,
		}
		join.OnCond = IdentifierWriter{
			Quote:      gen.Options.CaseSensitive,
			Namespace:  fkey.Target,
			Identifier: gen.DB.GetTable(fkey.Target).PK,
		}
		join.Equals = IdentifierWriter{
			Quote:      gen.Options.CaseSensitive,
			Namespace:  gen.FromName,
			Identifier: fkey.Key,
		}

		fn(join)
		return true
	})
}

type GroupByWriter struct {
	*SQLGenerator
}

func (w GroupByWriter) ShouldWrite() bool {
	if w.SQLGenerator.Options.ForceGroupBy {
		return true
	}
	for _, s := range w.SQLGenerator.Selects {
		if s.Aggregate != "" {
			return true
		}
	}
	return false
}

func (w GroupByWriter) Write(b *strings.Builder) {
	if !w.ShouldWrite() {
		return
	}
	b.WriteString("\ngroup by\n")
	comma := MakeLoopSeparatorJoiner(",\n")
	indent := IndentWriter{1}
	for _, s := range w.SQLGenerator.Selects {
		if s.Aggregate != "" {
			continue
		}
		ident := IdentifierWriter{
			Quote:      w.SQLGenerator.Options.CaseSensitive,
			Identifier: s.Column,
			Namespace:  s.Table,
		}
		WriteAll(b, comma, indent, ident)
	}
}

type LoopSeparatorJoiner struct {
	First bool
	Join  Writer
}

func MakeLoopSeparatorJoiner(join string) *LoopSeparatorJoiner {
	return MakeLoopSeparatorJoinerWith(TextWriter{join})
}

func MakeLoopSeparatorJoinerWith(join Writer) *LoopSeparatorJoiner {
	return &LoopSeparatorJoiner{
		First: true,
		Join:  join,
	}
}

func (sep *LoopSeparatorJoiner) Write(b *strings.Builder) {
	if sep.First {
		sep.First = false
	} else {
		sep.Join.Write(b)
	}
}

type IndentWriter struct {
	Level int
}

func (w IndentWriter) Write(b *strings.Builder) {
	for i := 0; i < w.Level; i++ {
		b.WriteString("    ")
	}
}

func (gen *SQLGenerator) Write(b *strings.Builder) {
	b.WriteString("select\n")
	comma := MakeLoopSeparatorJoiner(",\n")
	indent := IndentWriter{1}
	for _, s := range gen.Selects {
		comma.Write(b)
		indent.Write(b)
		sw := SelectWriter{s, gen.Options}
		sw.Write(b)
	}
	b.WriteString("\nfrom ")
	from := IdentifierWriter{
		Quote:      gen.Options.CaseSensitive,
		Identifier: gen.FromName,
	}
	join := JoinWriter{gen}
	groupBy := GroupByWriter{gen}
	WriteAll(b, from, join, groupBy)
}
