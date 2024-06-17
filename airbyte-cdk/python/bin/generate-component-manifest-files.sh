#!/usr/bin/env bash

set -e

ROOT_DIR=${ROOT_DIR:-$(git rev-parse --show-toplevel)}
[ -z "$ROOT_DIR" ] && exit 1

YAML_DIR=airbyte-cdk/python/airbyte_cdk/sources/declarative
OUTPUT_DIR=airbyte-cdk/python/airbyte_cdk/sources/declarative/models

function main() {
  # TODO: We have to run the old version of datamodel_code_generator to get _actual_ pydantic v1 models.
  # We also update the imports below. Remove this and use the latest datamodel-codegen
  # when we properly update them to pydantic v2.
  pip install datamodel_code_generator==0.11.19
  rm -rf "$ROOT_DIR/$OUTPUT_DIR"/*.py
  echo "# generated by generate-component-manifest-files" > "$ROOT_DIR/$OUTPUT_DIR"/__init__.py

  for f in "$ROOT_DIR/$YAML_DIR"/*.yaml; do
    filename_wo_ext=$(basename "$f" | cut -d . -f 1)
    echo "from .$filename_wo_ext import *" >> "$ROOT_DIR/$OUTPUT_DIR"/__init__.py

    datamodel-codegen \
      --input "$ROOT_DIR/$YAML_DIR/$filename_wo_ext.yaml" \
      --output "$ROOT_DIR/$OUTPUT_DIR/$filename_wo_ext.py" \
      --disable-timestamp \
      --enum-field-as-literal one \
      --set-default-enum-member

    # There is a limitation of Pydantic where a model's private fields starting with an underscore are inaccessible.
    # The Pydantic model generator replaces special characters like $ with the underscore which results in all
    # component's $parameters field resulting in _parameters. We have been unable to find a workaround in the
    # model generator or while reading the field. There is no estimated timeline on the fix even though it is
    # widely debated here:
    # https://github.com/pydantic/pydantic/issues/288.
    #
    # Our low effort way to address this is to perform additional post-processing to rename _parameters to parameters.
    # We can revisit this if there is movement on a fix.
    #
    # We update the pydantic imports because we are generating v1 models (via an older version of datamodel-codegen
    # that uses v1 pydantic dependency, but the CDK uses Pydantic v2 to process v2 protocol models. Therefore we
    # need to update these imports to use pydantic.v1
    temp_file=$(mktemp)
    sed -e 's/ _parameters:/ parameters:/g' -e 's/from pydantic/from pydantic.v1/g' "$ROOT_DIR/$OUTPUT_DIR/$filename_wo_ext.py" > "${temp_file}"
    output_file="$ROOT_DIR/$OUTPUT_DIR/$filename_wo_ext.py"
    mv "${temp_file}" "${output_file}"
    echo "Generated component manifest files into '${output_file}'."
  done
}

main "$@"
