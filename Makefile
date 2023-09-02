# Go parameters
GOCMD=go
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

all: test

test:
	$(GOTEST) -v ./...

clean:
	$(GOMOD) tidy
	rm -rf ./testdata

deps:
	$(GOGET) -u

lint:
	golint ./...

vet:
	$(GOCMD) vet ./...

fmt:
	gofmt -s -w .

tidy:
	$(GOMOD) tidy

# Run all code checks
check: lint vet fmt test

# Download all dependencies
prepare:
	$(GOMOD) download

# Update all dependencies
update:
	$(GOMOD) tidy
	$(GOMOD) download
