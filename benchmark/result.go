package benchmark

import (
	"fmt"
	"os"
	"strings"
	"text/template"
)

type benchmarkCacheMemoryResult struct {
	bytesHeap float64
	bytesYAML float64
}

// heapRun holds (N, mb_heap) from one benchmarkCacheMemoryN run for estimating
// per-object size.
type heapRun struct {
	n int
	r benchmarkCacheMemoryResult
}

type resultData struct {
	perObj     float64
	baseline   float64
	yamlPerObj float64
	overhead   float64
}

// RunView is the template-facing view of a single benchmark run (exported for text/template).
type RunView struct {
	N         int
	HeapBytes float64
	YAMLBytes float64
	HeapKiB   float64
	YAMLKiB   float64
}

// DataView is the template-facing view of derived estimates (exported for text/template).
type DataView struct {
	Baseline   float64
	PerObj     float64
	YamlPerObj float64
	Overhead   float64
}

// PieBreakdown holds pie chart data for the markdown template.
type PieBreakdown struct {
	LastN       int
	BaselinePct float64
	PerObjPct   float64
}

// resultTemplateData is the root value passed to templates.
type resultTemplateData struct {
	Runs      []RunView
	Data      DataView
	TotalHeap float64
	TotalYAML float64
	Pie       *PieBreakdown // nil if no runs
}

func buildTemplateData(runs []heapRun, data resultData) resultTemplateData {
	runsView := make([]RunView, len(runs))
	var totalHeap, totalYAML float64
	for i, r := range runs {
		heapKiB := r.r.bytesHeap / 1024
		yamlKiB := r.r.bytesYAML / 1024
		totalHeap += r.r.bytesHeap
		totalYAML += r.r.bytesYAML
		runsView[i] = RunView{
			N:         r.n,
			HeapBytes: r.r.bytesHeap,
			YAMLBytes: r.r.bytesYAML,
			HeapKiB:   heapKiB,
			YAMLKiB:   yamlKiB,
		}
	}
	out := resultTemplateData{
		Runs: runsView,
		Data: DataView{
			Baseline:   data.baseline,
			PerObj:     data.perObj,
			YamlPerObj: data.yamlPerObj,
			Overhead:   data.overhead,
		},
		TotalHeap: totalHeap,
		TotalYAML: totalYAML,
	}
	if len(runs) > 0 {
		last := runs[len(runs)-1]
		modeledHeap := data.baseline + float64(last.n)*data.perObj
		baselinePct := 0.0
		if modeledHeap > 0 {
			baselinePct = data.baseline / modeledHeap * 100
		}
		out.Pie = &PieBreakdown{
			LastN:       last.n,
			BaselinePct: baselinePct,
			PerObjPct:   100 - baselinePct,
		}
	}
	return out
}

// Template helper functions (take []RunView so templates can pass .Runs).
func runCountsStringView(runs []RunView) string {
	var b strings.Builder
	for i, r := range runs {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%d", r.N)
	}
	return b.String()
}

func runCountsQuotedView(runs []RunView) string {
	var parts []string
	for _, r := range runs {
		parts = append(parts, fmt.Sprintf("\"%d\"", r.N))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func maxHeapView(runs []RunView) string {
	var max float64
	for _, r := range runs {
		if r.HeapBytes > max {
			max = r.HeapBytes
		}
	}
	return fmt.Sprintf("%.0f", max)
}

func maxYAMLView(runs []RunView) string {
	var max float64
	for _, r := range runs {
		if r.YAMLBytes > max {
			max = r.YAMLBytes
		}
	}
	return fmt.Sprintf("%.0f", max)
}

func heapLineValues(runs []RunView) string {
	var parts []string
	for _, r := range runs {
		parts = append(parts, fmt.Sprintf("%.0f", r.HeapBytes))
	}
	return strings.Join(parts, ", ")
}

func yamlLineValues(runs []RunView) string {
	var parts []string
	for _, r := range runs {
		parts = append(parts, fmt.Sprintf("%.0f", r.YAMLBytes))
	}
	return strings.Join(parts, ", ")
}

var resultTemplateFuncs = template.FuncMap{
	"runCountsString": runCountsStringView,
	"runCountsQuoted": runCountsQuotedView,
	"maxHeap":         maxHeapView,
	"maxYAML":         maxYAMLView,
	"heapLineValues":  heapLineValues,
	"yamlLineValues":  yamlLineValues,
	"div":             div,
	"toFloat64":       toFloat64,
}

func printResults(runs []heapRun, format OutputFormat) error {
	perObj, baseline := estimatePerObj(runs)

	// Average YAML size per object, ex. the total YAML bytes / total objects
	// across runs.
	var sumYAML, sumN float64
	for _, r := range runs {
		sumYAML += r.r.bytesYAML
		sumN += float64(r.n)
	}
	yamlPerObj := sumYAML / sumN
	overhead := perObj - yamlPerObj

	fileName := ""

	switch format {
	case OutputFormatCSV:
		fileName = "templates/csv.tpl"
	case OutputFormatMarkdown:
		fileName = "templates/markdown.tpl"
	case OutputFormatText:
		fileName = "templates/text.tpl"
	}

	tpl, err := getTemplate(fileName)
	if err != nil {
		return fmt.Errorf("failed to get template: %w", err)
	}

	if err := tpl.Execute(
		os.Stdout,
		buildTemplateData(runs, resultData{
			perObj:     perObj,
			baseline:   baseline,
			yamlPerObj: yamlPerObj,
			overhead:   overhead,
		})); err != nil {
		return fmt.Errorf("failed to execute template: %w", err)
	}

	return nil
}

func getTemplate(fileName string) (*template.Template, error) {
	content, err := templates.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file: %w", err)
	}
	return template.New(
		fileName,
	).Funcs(
		resultTemplateFuncs,
	).Parse(
		string(content),
	)
}

// estimatePerObj performs linear regression, ex.:
//
//	heap ≈ baselineBytes + n*perObjBytes
//
// Returns (perObjBytes, baselineBytes). Uses least squares and returns
// (0, 0) if fewer than 2 points.
func estimatePerObj(runs []heapRun) (perObjBytes, baselineBytes float64) {
	if len(runs) < 2 {
		return 0, 0
	}
	var sumN, sumH, sumN2, sumNH float64
	for _, r := range runs {
		n, h := float64(r.n), r.r.bytesHeap
		sumN += n
		sumH += h
		sumN2 += n * n
		sumNH += n * h
	}
	k := float64(len(runs))
	denom := k*sumN2 - sumN*sumN
	if denom == 0 {
		return 0, 0
	}
	perObjBytes = (k*sumNH - sumN*sumH) / denom
	baselineBytes = (sumH - perObjBytes*sumN) / k
	return perObjBytes, baselineBytes
}

// toFloat64 converts template numeric types to float64 for div.
func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	default:
		return 0
	}
}

func div(a, b interface{}, c ...interface{}) float64 {
	fa := toFloat64(a)
	fb := toFloat64(b)
	for _, x := range c {
		fb *= toFloat64(x)
	}
	if fb == 0 {
		return 0
	}
	return fa / fb
}
