package packeter

// ChannelDispatcher is a simple wrapper around map[ID]chan<- T. Use it to dispatch
// messages by ID to an appropriate channel. It's recommended to use buffered channels of 1 to
// avoid blocking. Once message is dispatched, receiver is unregistered.
type ChannelDispatcher[ID comparable, T any] struct {
	receivers map[ID]chan<- T
}

func NewChannelDispatcher[ID comparable, T any]() *ChannelDispatcher[ID, T] {
	return &ChannelDispatcher[ID, T]{receivers: make(map[ID]chan<- T)}
}

func (cd *ChannelDispatcher[ID, T]) RegisterReceiver(id ID, ch chan<- T) {
	if _, ok := cd.receivers[id]; ok {
		panic("receiver with given id already exists, delete the existing receiver before setting a new one")
	}
	cd.receivers[id] = ch
}

func (cd *ChannelDispatcher[ID, T]) DispatchTo(id ID, v T) {
	if ch, ok := cd.receivers[id]; ok {
		ch <- v
		delete(cd.receivers, id)
	}
}

func (cd *ChannelDispatcher[ID, T]) DispatchToAll(v T) {
	for _, ch := range cd.receivers {
		ch <- v
	}
	// I think there was a talk in Go about adding a built-in primitive to clear the map. If it's added,
	// replace the following statement with it.
	cd.receivers = make(map[ID]chan<- T)
}
