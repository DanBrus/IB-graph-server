import json
import os
from typing import Dict, Any, Optional


class TemplateDriverError(Exception):
    """Base class for all template driver errors."""
    pass


class SpecificationError(TemplateDriverError):
    """Raised when there is a problem with specification.json or its contents."""
    pass


class OperationError(TemplateDriverError):
    """Raised for errors related to a specific operation (unknown op, bad params, etc.)."""
    pass


class TemplateFileError(TemplateDriverError):
    """Raised when a template file is missing or cannot be read."""
    pass


class TypeQLTemplateDriver:
    """
    JSON-based driver that loads operation specification and TypeQL templates from db/{version}.

    Directory layout example:
      db/
        0.1/
          specification.json
          node-create.tql
          node-update.tql
          node-delete.tql
          ...

    specification.json is an array of objects like:

    [
      {
        "operation": "node-create",
        "file": "node-create.tql",
        "description": "Creates a node within the specified investigation board version.",
        "params": [
          "investigation_name",
          "version",
          "node_id",
          "name",
          "pos_x",
          "pos_y",
          "picture_path",
          "description"
        ],
        "output": null
      },
      ...
    ]
    """

    def __init__(self, db_root: str, version: str, spec_filename: str = "specification.json") -> None:
        """
        :param db_root: Path to directory containing versioned db folders (e.g. "db").
        :param version: Version folder name (e.g. "0.1" or "v0.1").
        :param spec_filename: Name of the specification file inside db/{version}.
        :raises SpecificationError: if specification file is missing or invalid.
        :raises TemplateFileError: if any template file listed in spec is missing or unreadable.
        """
        self._version = version
        self._base_dir = os.path.join(db_root, version)

        if not os.path.isdir(self._base_dir):
            raise SpecificationError(
                f'Version directory "{self._base_dir}" does not exist or is not a directory.'
            )

        spec_path = os.path.join(self._base_dir, spec_filename)

        if not os.path.isfile(spec_path):
            raise SpecificationError(
                f'Specification file "{spec_path}" does not exist or is not a file.'
            )

        try:
            with open(spec_path, "r", encoding="utf-8") as f:
                spec_data = json.load(f)
        except Exception as e:
            raise SpecificationError(f'Failed to read specification file "{spec_path}": {e}') from e

        if not isinstance(spec_data, list):
            raise SpecificationError(
                f'Specification file "{spec_path}" must contain a JSON array of operations.'
            )

        # Internal structure:
        # self._operations[name] = {
        #   "file": str,              # template filename
        #   "params": set[str],       # required parameter names
        #   "description": str|None,
        #   "output": Any,
        #   "template": str           # loaded template content
        # }
        self._operations: Dict[str, Dict[str, Any]] = {}

        for entry in spec_data:
            self._process_spec_entry(entry)

    def _process_spec_entry(self, entry: Dict[str, Any]) -> None:
        """Validate a single specification entry and load its template file."""
        if not isinstance(entry, dict):
            raise SpecificationError(
                f"Each specification entry must be an object, got: {type(entry).__name__}"
            )

        operation = entry.get("operation")
        filename = entry.get("file")
        params = entry.get("params")

        if not operation or not isinstance(operation, str):
            raise SpecificationError(f"Specification entry is missing a valid 'operation' field: {entry}")
        if not filename or not isinstance(filename, str):
            raise SpecificationError(f"Specification entry for '{operation}' is missing a valid 'file' field.")
        if params is None or not isinstance(params, list) or not all(
            isinstance(p, str) for p in params
        ):
            raise SpecificationError(
                f"Specification entry for '{operation}' has invalid 'params' field; "
                f"it must be a list of strings."
            )

        if operation in self._operations:
            raise SpecificationError(f"Duplicate operation name in specification: '{operation}'")

        template_path = os.path.join(self._base_dir, filename)

        if not os.path.isfile(template_path):
            raise TemplateFileError(
                f'Template file "{template_path}" for operation "{operation}" does not exist.'
            )

        try:
            with open(template_path, "r", encoding="utf-8") as tf:
                template_content = tf.read()
        except Exception as e:
            raise TemplateFileError(
                f'Failed to read template file "{template_path}" for operation "{operation}": {e}'
            ) from e

        self._operations[operation] = {
            "file": filename,
            "params": set(params),
            "description": entry.get("description"),
            "output": entry.get("output"),
            "template": template_content,
        }

    def has_operation(self, operation: str) -> bool:
        """Return True if given operation is defined in specification."""
        return operation in self._operations

    def required_params(self, operation: str) -> set:
        """
        Return set of required parameter names for the given operation.
        :raises OperationError: if operation is unknown.
        """
        op = self._operations.get(operation)
        if op is None:
            raise OperationError(f'Unknown operation "{operation}".')
        return set(op["params"])

    def get_operation(
        self,
        operation: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> str:
        """
        Build a TypeQL query string for the given operation and parameters.

        :param operation: Operation name from the specification (e.g. "node-create").
        :param params: Dictionary of parameters for template formatting (optional).
        :param kwargs: Alternative way to pass parameters as keyword arguments.
        :return: TypeQL query string with all placeholders filled.
        :raises OperationError:
            - if operation is not defined,
            - if required parameters are missing,
            - if unknown parameters are provided.
        """
        op = self._operations.get(operation)
        if op is None:
            raise OperationError(f'Unknown operation "{operation}".')

        # Merge dict + kwargs into a single params dict
        merged_params: Dict[str, Any] = {}
        if params:
            if not isinstance(params, dict):
                raise OperationError(
                    f'Parameters for operation "{operation}" must be a dict, got {type(params).__name__}.'
                )
            merged_params.update(params)
        if kwargs:
            # kwargs override values from params dict if there is a conflict
            merged_params.update(kwargs)

        required = op["params"]
        provided = set(merged_params.keys())

        missing = required - provided
        extra = provided - required

        if missing:
            missing_list = ", ".join(sorted(missing))
            raise OperationError(
                f'Operation "{operation}" is missing required parameter(s): {missing_list}.'
            )

        if extra:
            extra_list = ", ".join(sorted(extra))
            raise OperationError(
                f'Operation "{operation}" received unknown parameter(s): {extra_list}.'
            )

        template = op["template"]

        try:
            # TypeQL template should use Python's str.format placeholders: {param_name}
            query = template.format(**merged_params)
        except KeyError as e:
            # Should not happen if we validated above, but keep a clear error just in case.
            raise OperationError(
                f'Missing parameter "{e.args[0]}" when formatting template for operation "{operation}".'
            ) from e
        except Exception as e:
            raise OperationError(
                f'Failed to format template for operation "{operation}": {e}'
            ) from e

        return query

    def describe_operation(self, operation: str) -> Dict[str, Any]:
        """
        Return a dict with metadata about the operation:
        { "operation", "file", "description", "params", "output" }.

        :raises OperationError: if operation is unknown.
        """
        op = self._operations.get(operation)
        if op is None:
            raise OperationError(f'Unknown operation "{operation}".')

        return {
            "operation": operation,
            "file": op["file"],
            "description": op.get("description"),
            "params": sorted(list(op["params"])),
            "output": op.get("output"),
        }

    @property
    def version(self) -> str:
        """Return the version string used for this driver."""
        return self._version

    @property
    def base_dir(self) -> str:
        """Return the base directory path where templates/specification are stored."""
        return self._base_dir
