from collections import defaultdict, namedtuple

from django.forms.models import ModelForm
from isic_metadata.metadata import MetadataRow
import pandas as pd
from pydantic.main import BaseModel
from s3_file_field.widgets import S3FileInput

from isic.ingest.models import Accession, MetadataFile


class MetadataForm(ModelForm):
    class Meta:
        model = MetadataFile
        fields = ["blob"]
        widgets = {"blob": S3FileInput(attrs={"accept": "text/csv"})}


class Problem(BaseModel):
    message: str | None
    context: list | None
    type: str | None = "error"


# this provides structure for mapping a row in a csv (or column in a dataframe) to a
# field in the database, and the query expression to use when looking up the value.
# csv_field, db_lookup_field, db_bulk_lookup_expr
IdentifierMap = namedtuple("IdentifierMap", ["field", "db_lookup", "db_lookup_bulk"])


def validate_csv_format_and_filenames(df, cohort):
    problems = []

    # TODO: duplicate columns

    if "isic_id" in df.columns and "filename" in df.columns:
        problems.append(Problem(message="Cannot provide both isic_id and filename columns."))
        return problems
    elif "isic_id" not in df.columns and "filename" not in df.columns:
        problems.append(Problem(message="Must provide either isic_id or filename column."))
        return problems
    else:
        if "isic_id" in df.columns:
            identifier: IdentifierMap = IdentifierMap(
                "isic_id", "image__isic_id", "image__isic_id__in"
            )
        else:
            identifier: IdentifierMap = IdentifierMap(
                "filename", "original_blob_name", "original_blob_name__in"
            )

    duplicate_rows = df[df[identifier.field].duplicated()][identifier.field].values
    if duplicate_rows.size:
        problems.append(
            Problem(message=f"Duplicate {identifier.field}s found.", context=list(duplicate_rows))
        )

    matching_accessions = Accession.objects.filter(
        **{"cohort": cohort, identifier.db_lookup_bulk: df[identifier.field].values}
    ).values_list(identifier.db_lookup, flat=True)

    existing_df = pd.DataFrame([x for x in matching_accessions], columns=[identifier.field])
    unknown_images = set(df[identifier.field].values) - set(existing_df[identifier.field].values)
    if unknown_images:
        problems.append(
            Problem(
                message="Encountered unknown images in the CSV.",
                context=list(unknown_images),
                type="warning",
            )
        )

    return problems


def validate_internal_consistency(df):
    # keyed by column, message
    column_problems: dict[tuple[str, str], list[int]] = defaultdict(list)

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        try:
            MetadataRow.parse_obj(row)
        except Exception as e:
            for error in e.errors():
                column = error["loc"][0]
                column_problems[(column, error["msg"])].append(i)

    # TODO: defaultdict doesn't work in django templates?
    return dict(column_problems)


def validate_archive_consistency(df, cohort):
    # it's okay to assume that these are the only two possibilities since df has already
    # passed validate_csv_format_and_filenames.
    if "isic_id" in df.columns:
        identifier: IdentifierMap = IdentifierMap("isic_id", "image__isic_id", "image__isic_id__in")
    else:
        identifier: IdentifierMap = IdentifierMap(
            "filename", "original_blob_name", "original_blob_name__in"
        )

    # keyed by column, message
    column_problems: dict[tuple[str, str], list[int]] = defaultdict(list)
    accessions = Accession.objects.filter(
        **{"cohort": cohort, identifier.db_lookup_bulk: df[identifier.field].values}
    ).values_list(identifier.db_lookup, "metadata")
    # TODO: easier way to do this?
    accessions_dict = {x[0]: x[1] for x in accessions}

    for i, (_, row) in enumerate(df.iterrows(), start=2):
        existing = accessions_dict[row[identifier.field]]
        row = existing | {k: v for k, v in row.items() if v is not None}

        try:
            MetadataRow.parse_obj(row)
        except Exception as e:
            for error in e.errors():
                column = error["loc"][0]

                column_problems[(column, error["msg"])].append(i)

    # TODO: defaultdict doesn't work in django templates?
    return dict(column_problems)
