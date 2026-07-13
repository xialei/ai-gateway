package server

// Context plugin registration point.
//
// The Context Pipeline (internal/context) ships an engine + Plugin contract
// but NO built-in plugins. Memory / RAG / Rewrite / Summary are design
// non-goals for the gateway core: they require embedding, retrieval, and
// summarization the gateway deliberately does not implement. Real context
// plugins are implemented by the upper layer per the produces/consumes
// contract (see internal/context.Plugin) and registered here.
//
// To wire an upper-layer plugin, build it from config and append to the
// plugin slice passed to ctxpipe.NewEngine in server.New, e.g.:
//
//	plugins := []ctxpipe.Plugin{mymemory.New(cfg), myrag.New(cfg)}
//	ce, err := ctxpipe.NewEngine(plugins, breakers, logger)
//
// With none registered the engine is a no-op and adds no per-request cost.
