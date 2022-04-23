all: push

ifeq ($(VERSION),)
    VERSION = 0.1
endif

TAG = $(VERSION)
REPO = camelinx
IMAGE = azsvcbusbench
BINNAME = build/azsvcbusbench
CURDIR = $(shell pwd)

PREFIX = $(REPO)/$(IMAGE)

DOCKER_RUN = docker run --rm -v $(CURDIR)/../:/go/src/github.com -w /go/src/github.com/azsvcbusbench/
GOLANG_CONTAINER = golang:1.18
DOCKERFILE = build/Dockerfile

azsvcbusbench:
	$(DOCKER_RUN) -e CGO_ENABLED=0 $(GOLANG_CONTAINER) go build -ldflags "-w -X main.version=${VERSION}" -o $(BINNAME) github.com/azsvcbusbench/cmd/azsvcbusbench

test:
	$(DOCKER_RUN) $(GOLANG_CONTAINER) go test -v ./...

image: azsvcbusbench
	docker build -f $(DOCKERFILE) -t $(PREFIX):$(TAG) .

push: image
	docker push $(PREFIX):$(TAG)

clean:
	rm -f $(BINNAME)
	docker rmi $(PREFIX):$(TAG)
