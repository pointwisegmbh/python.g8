DOCKER = docker
PYTHON = python
REPO = $docker_repo$
NAME = $name$

GIT_VERSION := \$(shell git describe --abbrev=7 --dirty --always --tags)

TAG = \$(GIT_VERSION)

test:
	\$(PYTHON) -m unittest discover

docker:
	\$(DOCKER) build -t \$(NAME) .

build:
	 make docker

push:
	\$(DOCKER) push \$(REPO)/\$(NAME):latest
	\$(DOCKER) push \$(REPO)/\$(NAME):\$(TAG)

tag:
	\$(DOCKER) tag \$(NAME) \$(REPO)/\$(NAME):latest
	\$(DOCKER) tag \$(NAME) \$(REPO)/\$(NAME):\$(TAG)

run:
	\$(DOCKER) run --name \$(NAME) -d -p 8080:80 \$(NAME)

stop:
	\$(DOCKER) rm -f \$(NAME)

rm_containers:
	\$(DOCKER) ps -aq | xargs \$(DOCKER) rm || true

rmi_images:
	\$(DOCKER) rmi -f \$(NAME) \$(REPO)/\$(NAME):latest \$(REPO)/\$(NAME):\$(TAG) || true

venv:
	eval \$(source venv/bin/activate)
