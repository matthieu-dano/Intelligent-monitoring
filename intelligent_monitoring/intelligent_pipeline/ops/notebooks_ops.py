import os
import re

import dagstermill
import nbformat
from dagster import Field, Nothing, Out, String, op
from nbconvert import PDFExporter

from .logger import logging_decorator


class NotebookOps:
    @staticmethod
    def create_notebook_run_op(op_name: str):
        notebook_path = os.path.join(os.getenv("NOTEBOOK_PATH")) + "/" + op_name + ".ipynb"
        output_notebook_name = op_name + "_output"
        notebook_op = dagstermill.define_dagstermill_op(
            name="notebook_run_" + op_name+"_op",
            notebook_path=notebook_path,
            output_notebook_name=output_notebook_name,
            config_schema={
                "date": Field(String, description="Date for the execution of the op"),
                "metric": Field(list, description="Metric name used in the query"),
                "notebook_name": Field(String, description="Name of the notebook to execute"),
                "tag": Field(dict, description="Tags for the notebook"),
            },
            required_resource_keys={"checkpoint", "skyminer_api", "output_notebook_io_manager"},
        )
        return notebook_op

    @staticmethod
    def create_notebook_export_op(op_name: str):
        @op(
            name="notebook_export_" + op_name+"_op",
            out={"result": Out(Nothing, description="Indicates completion of the export")},
            description="Export a notebook to PDF",
            tags={"operation": "export_notebook"},
            config_schema={
                "notebook_name": Field(String, description="Name of the notebook to execute"),
                "logs": Field(list, description=""),
                "date": Field(String, description="Date of the execution")
            },
        )
        @logging_decorator
        def notebook_export(context) -> None:
            notebook_name = context.op_config["notebook_name"]
            date = context.op_config["date"]
            logs = context.op_config["logs"]
            
            context.log.info(f"Exporting notebook: {notebook_name} to PDF")

            # Convert the logs list into a string
            logs_str = " ".join([str(entry) for entry in logs])
            
            context.log.info(f"Logs: {logs}")

            # Define the pattern to extract the notebook path
            path_pattern = re.compile(r"path='([^']+\.ipynb)'")

            # Search for the notebook path in logs_str
            match = path_pattern.search(logs_str)
            if match:
                output_notebook_name = match.group(1)
            else:
                raise ValueError("No notebook path found in logs.")

            # Define the output path for the PDF file
            notebook_dir = os.getenv("NOTEBOOK_PATH", "")
            output_pdf_name = os.path.join(
                notebook_dir, f"{notebook_name}_{date}.pdf"
            )

            context.log.info(
                f"Converting notebook: {output_notebook_name} to PDF: {output_pdf_name}"
            )

            # Determine the notebook path based on the operating system
            if os.name == 'nt':  # Windows
                notebook_path = os.path.join("C:", output_notebook_name)
            else:  # Linux/Unix
                notebook_path = output_notebook_name

            # Load the notebook from the path
            with open(notebook_path, "r", encoding="utf-8") as f:
                nb = nbformat.read(f, as_version=4)

            # Filter cells based on tags
            filtered_cells = [
                cell
                for cell in nb.cells
                if cell.metadata.get("tags", []) == []
                or cell.metadata.get("tags", []) == ["skyminer_hide_code"]
            ]
            nb.cells = filtered_cells

            # Set up the PDF exporter
            pdf_exporter = PDFExporter()

            # Convert the notebook to PDF
            (body, resources) = pdf_exporter.from_notebook_node(nb)

            # Write the resulting PDF
            context.log.info(f"Writing PDF: {output_pdf_name}")
            with open(output_pdf_name, "wb") as f:
                f.write(body)

            context.log.info(f"Conversion successful. PDF saved to: {output_pdf_name}")

        return notebook_export
