all: push

ifeq ($(VERSION),)
    VERSION = 0.1
endif

TAG = $(VERSION)
REPO = camelinx
IMAGE = azsvcbusbench
BINDIR = bin
CURDIR = $(shell pwd)

PREFIX = $(REPO)/$(IMAGE)

DOCKER_RUN = docker run --rm -v $(CURDIR)/../:/go/src/github.com -w /go/src/github.com/azsvcbusbench/
GOLANG_CONTAINER = golang:1.18
DOCKERFILE = build/Dockerfile

azsvcbusbench:
	mkdir -p $(BINDIR)
	$(DOCKER_RUN) -e CGO_ENABLED=0 $(GOLANG_CONTAINER) go build -ldflags "-w -X main.version=${VERSION}" -o $(BINDIR)/$@ github.com/azsvcbusbench/cmd/azsvcbusbench

idgen:
	mkdir -p $(BINDIR)
	$(DOCKER_RUN) -e CGO_ENABLED=0 $(GOLANG_CONTAINER) go build -ldflags "-w -X main.version=${VERSION}" -o $(BINDIR)/$@ github.com/azsvcbusbench/cmd/idgen

ipv4gen:
	mkdir -p $(BINDIR)
	$(DOCKER_RUN) -e CGO_ENABLED=0 $(GOLANG_CONTAINER) go build -ldflags "-w -X main.version=${VERSION}" -o $(BINDIR)/$@ github.com/azsvcbusbench/cmd/ipv4gen

test:
	$(DOCKER_RUN) $(GOLANG_CONTAINER) go test -v ./...

image: azsvcbusbench idgen ipv4gen
	docker build -f $(DOCKERFILE) -t $(PREFIX):$(TAG) .

push: image
	docker push $(PREFIX):$(TAG)

clean:
	rm -f $(BINDIR)/*
	docker rmi $(PREFIX):$(TAG)
