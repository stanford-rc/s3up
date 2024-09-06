package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
)

// profilers enables any optional cpu, memory, or trace profiling outputs, and
// returns a shutdown function to be called when the program exits.
func profilers(opts *Options) (shutdown func(), err error) {
	var cpuProfileOut *os.File
	var memProfileOut *os.File
	var traceOut *os.File

	shutdown = func() {
		if cpuProfileOut != nil {
			pprof.StopCPUProfile()
			cpuProfileOut.Close()
		}
		if memProfileOut != nil {
			runtime.GC()
			err = pprof.WriteHeapProfile(memProfileOut)
			if err != nil {
				log.Printf("unable to write memory profile: %s", err)
			}
			memProfileOut.Close()
		}
		if traceOut != nil {
			trace.Stop()
			traceOut.Close()
		}
	}

	if opts.CpuProfile != "" {
		cpuProfileOut, err = os.Create(opts.CpuProfile)
		if err != nil {
			shutdown()
			return nil, err
		} else {
			err = pprof.StartCPUProfile(cpuProfileOut)
			if err != nil {
				shutdown()
				return nil, err
			}
		}
	}

	if opts.MemProfile != "" {
		memProfileOut, err = os.Create(opts.MemProfile)
		if err != nil {
			shutdown()
			return nil, err
		}
	}

	if opts.Trace != "" {
		traceOut, err = os.Create(opts.Trace)
		if err != nil {
			shutdown()
			return nil, err
		} else {
			err = trace.Start(traceOut)
			if err != nil {
				shutdown()
				return nil, err
			}
		}
	}

	return shutdown, nil
}
