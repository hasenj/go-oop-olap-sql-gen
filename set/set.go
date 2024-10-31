package set

type FnMatch[T any] func(item T) bool
type FnEqual[T any] func(item1 T, item2 T) bool

type Set[T any] struct {
	items []T
	eq    FnEqual[T]
}

func MakeSet[T any](eq FnEqual[T]) Set[T] {
	return Set[T]{
		eq: eq,
	}
}

func EqualityMatcher[T any](eq FnEqual[T], item T) FnMatch[T] {
	return func(item0 T) bool {
		return eq(item, item0)
	}
}

func (s *Set[T]) IndexWith(match FnMatch[T]) int {
	for index, item := range s.items {
		if match(item) {
			return index
		}
	}
	return -1
}

func (s *Set[T]) Index(item T) int {
	return s.IndexWith(EqualityMatcher(s.eq, item))
}

func (s *Set[T]) Find(match FnMatch[T]) (result T, found bool) {
	index := s.IndexWith(match)
	if index == -1 {
		return
	} else {
		found = true
		result = s.items[index]
		return
	}
}

// returns false if already exists
func (s *Set[T]) Add(item T) bool {
	if s.Index(item) == -1 {
		s.items = append(s.items, item)
		return true
	} else {
		return false
	}
}

func (s *Set[T]) Count() int {
	return len(s.items)
}

func (s *Set[T]) Iter(fn func(item T) bool) {
	for _, item := range s.items {
		cont := fn(item)
		if !cont {
			break
		}
	}
}
