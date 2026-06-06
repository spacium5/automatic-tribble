//go:build !solution

package main

import (
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	digitSymbols = map[byte]string{
		'0': Zero,
		'1': One,
		'2': Two,
		'3': Three,
		'4': Four,
		'5': Five,
		'6': Six,
		'7': Seven,
		'8': Eight,
		'9': Nine,
	}
)

func main() {
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	http.HandleFunc("/", handleClock)
	_ = http.ListenAndServe(":"+*port, nil)
}

func handleClock(w http.ResponseWriter, r *http.Request) {
	k, err := parseK(r.URL.Query().Get("k"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	timeStr := r.URL.Query().Get("time")
	if timeStr == "" {
		timeStr = time.Now().Format("15:04:05")
	}

	if err := validateTime(timeStr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	img, err := renderClock(timeStr, k)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "image/png")
	_ = png.Encode(w, img)
}

func parseK(raw string) (int, error) {
	if raw == "" {
		return 1, nil
	}

	k, err := strconv.Atoi(raw)
	if err != nil || k < 1 || k > 30 {
		return 0, fmt.Errorf("invalid k")
	}
	return k, nil
}

func validateTime(s string) error {
	if len(s) != 8 || s[2] != ':' || s[5] != ':' {
		return fmt.Errorf("invalid time")
	}

	for i, ch := range s {
		if i == 2 || i == 5 {
			continue
		}
		if ch < '0' || ch > '9' {
			return fmt.Errorf("invalid time")
		}
	}

	hour, err := strconv.Atoi(s[0:2])
	if err != nil {
		return fmt.Errorf("invalid time")
	}
	minute, err := strconv.Atoi(s[3:5])
	if err != nil {
		return fmt.Errorf("invalid time")
	}
	second, err := strconv.Atoi(s[6:8])
	if err != nil {
		return fmt.Errorf("invalid time")
	}

	if hour > 23 || minute > 59 || second > 59 {
		return fmt.Errorf("invalid time")
	}

	return nil
}

func renderClock(timeStr string, k int) (image.Image, error) {
	symbols := make([]string, 0, 8)
	for i, ch := range timeStr {
		if i == 2 || i == 5 {
			symbols = append(symbols, Colon)
			continue
		}
		sym, ok := digitSymbols[byte(ch)]
		if !ok {
			return nil, fmt.Errorf("invalid time")
		}
		symbols = append(symbols, sym)
	}

	baseWidth := 0
	for _, sym := range symbols {
		baseWidth += symbolWidth(sym)
	}

	lines := strings.Split(Zero, "\n")
	baseHeight := len(lines)

	width := baseWidth * k
	height := baseHeight * k
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	xOffset := 0
	for _, sym := range symbols {
		drawSymbol(img, sym, xOffset, 0, k)
		xOffset += symbolWidth(sym)
	}

	return img, nil
}

func symbolWidth(sym string) int {
	return len(strings.SplitN(sym, "\n", 2)[0])
}

func drawSymbol(img *image.RGBA, sym string, xOff, yOff, k int) {
	lines := strings.Split(sym, "\n")
	for y, line := range lines {
		for x, ch := range line {
			var c color.Color = color.White
			if ch == '1' {
				c = Cyan
			}
			for dy := 0; dy < k; dy++ {
				for dx := 0; dx < k; dx++ {
					img.Set((xOff+x)*k+dx, (yOff+y)*k+dy, c)
				}
			}
		}
	}
}
