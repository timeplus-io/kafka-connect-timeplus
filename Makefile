BIN_NAME ?= timeplus-kafkaconnect-sink
VERSION ?= dev
IMAGE_NAME ?= $(BIN_NAME):$(VERSION)
DOCKER_ID_USER ?= timeplus

DATE=$(shell gdate -Iseconds)
COMMIT=$(shell git rev-parse --short HEAD)
FULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:dev_${COMMIT}
LATESTFULLNAME=$(DOCKER_ID_USER)/${BIN_NAME}:latest

build:
	mvn clean install
	mvn dependency:copy-dependencies

one_jar:
	mvn clean compile assembly:single

dep:
	mvn dependency:copy-dependencies

it:
	mvn test -Dtest=com.timeplus.kafkaconnect.integration.IntegrationTest

docker: Dockerfile one_jar
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t $(IMAGE_NAME) .

push:
	docker tag $(IMAGE_NAME) $(FULLNAME)
	docker push $(FULLNAME)
	docker tag $(IMAGE_NAME) $(LATESTFULLNAME)
	docker push $(LATESTFULLNAME)