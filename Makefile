work:
	go work use sdk-go
	go work use test
	go work use host

wasm:
	@cd test && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../host/test.wasm
wasm-prod:
	@cd test && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../host/test.prod.wasm -no-debug

test:
	@cd host && go test . -v -cover

bench:
	@cd host && go test -bench=. -v -run=Benchmark.*

cover:
	@mkdir -p _dist
	@cd host && go test . -coverprofile=../_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc . --exclude-dir=_example,_dist,internal,cmd --exclude-ext=pb.go

.PHONY: all test clean
