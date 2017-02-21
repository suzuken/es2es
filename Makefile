deps:
	which dep || go get github.com/golang/dep...

test:
	go test -v -tags=unit $$(go list ./... | grep -v /vendor/)
