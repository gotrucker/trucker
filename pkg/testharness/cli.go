package testharness

import (
	"fmt"
	"io"
	"strings"
	"time"
)

func Dispatch(args []string, projectPath string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage())
		return 2
	}

	switch args[0] {
	case "-gen":
		if len(args) != 4 || args[1] != "test" {
			fmt.Fprintln(stderr, usage())
			return 2
		}
		if err := Generate(projectPath, args[2], args[3]); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		fmt.Fprintf(stdout, "Generated test %s/tests/%s\n", args[2], args[3])
		return 0

	case "-test":
		if len(args) < 2 || len(args) > 4 || args[1] != "run" {
			fmt.Fprintln(stderr, usage())
			return 2
		}
		var truckFilter, testFilter string
		if len(args) >= 3 {
			truckFilter = args[2]
		}
		if len(args) == 4 {
			testFilter = args[3]
		}

		results, err := Run(projectPath, truckFilter, testFilter)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}

		failed := false
		for _, result := range results {
			fmt.Fprintf(stdout, "[%s] %s/%s (%s)\n", strings.ToUpper(result.Status), result.TruckName, result.TestName, result.Duration.Round(10*time.Millisecond))
			if result.Failed() {
				failed = true
				if result.Phase != "" {
					fmt.Fprintf(stdout, "  phase: %s\n", result.Phase)
				}
				if result.Error != "" {
					fmt.Fprintf(stdout, "  error: %s\n", result.Error)
				}
				if result.TargetOutputLSN != 0 {
					fmt.Fprintf(stdout, "  output_lsn: observed=%d target=%d\n", result.ObservedOutputLSN, result.TargetOutputLSN)
				}
				for _, failure := range result.Failures {
					fmt.Fprintf(stdout, "  %s\n", failure.String())
				}
			}
		}
		if failed {
			return 1
		}
		return 0

	default:
		fmt.Fprintln(stderr, usage())
		return 2
	}
}

func usage() string {
	return `Usage:
  trucker -gen test <truck> <test_name>
  trucker -test run [truck] [test_name]`
}
