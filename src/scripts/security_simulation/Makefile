SHELL := /bin/bash
SCRIPT_PATH := ./sh_scripts

.PHONY: port-forward check-pipeline open-pipeline \
kill-pipeline

port-forward:
	@bash $(SCRIPT_PATH)/port-forward.sh
	@#echo "✅  <make port-forward> done."

check-pipeline:
	@bash $(SCRIPT_PATH)/check-pipeline.sh
	@#echo "✅  <make check-pipeline> done."

open-pipeline: port-forward check-pipeline
	@echo "✅  <make open-pipeline> done."

kill-pipeline:
	@echo "正在停止 port-forward ..."
	@-pkill -f "kubectl port-forward" 2>/dev/null || true
	@echo "✅  <make kill-pipeline> done."
