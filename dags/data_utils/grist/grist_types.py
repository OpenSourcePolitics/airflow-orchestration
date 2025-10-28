import json

import sqlalchemy
import re
import pandas as pd
from .grist_helper import sanitize_identifier


class GristTypes:
    expr_easy_types = [
        (re.compile("^Text$"), sqlalchemy.Text()),
        (re.compile("^Numeric$"), sqlalchemy.Float()),
        (re.compile("^Int$"), sqlalchemy.Integer()),
        (re.compile("^Bool$"), sqlalchemy.Boolean()),
        (re.compile("^Date$"), sqlalchemy.Date()),
        (re.compile("^DateTime:(.*)$"), sqlalchemy.DateTime()),
        (re.compile("^Choice$"), sqlalchemy.String()),
        (re.compile("^Any$"), sqlalchemy.Text()),
    ]
    expr_choicelist = re.compile("^ChoiceList$")
    expr_ref = re.compile("^Ref:(.*)$")
    expr_reflist = re.compile("^RefList:(.*)$")

    def __init__(self, id, grist_fields, explode=False):
        self.id = id
        self.multiple = False
        self.explode = explode
        self.ref_table = None
        self.exploded_ref_table = None
        type_str = grist_fields["type"]
        for expr, t in GristTypes.expr_easy_types:
            if expr.match(type_str):
                self.sql_type = t
                if explode:
                    raise ValueError(
                        f"column {id} of type {type_str} cannot be exploded"
                    )
                return

        match_ref = GristTypes.expr_ref.match(type_str)
        if match_ref:
            if explode:
                raise ValueError(f"column {id} of type `ref` cannot be exploded")
            self.ref_table = sanitize_identifier(match_ref.group(1))
            self.sql_type = sqlalchemy.Integer()
            return

        match_reflist = GristTypes.expr_reflist.match(type_str)
        if match_reflist:
            self.multiple = True
            # TODO: use json instead of string.
            # metabase could in theory handle it
            self.sql_type = sqlalchemy.String()
            self.exploded_sql_type = sqlalchemy.Integer()
            self.exploded_ref_table = sanitize_identifier(match_reflist.group(1))
            return

        if GristTypes.expr_choicelist.match(type_str):
            self.multiple = True
            # TODO: use json instead of string.
            # metabase could in theory handle it
            self.sql_type = sqlalchemy.String()
            self.exploded_sql_type = sqlalchemy.String()
            return

        print(f"Type {type_str} not supported, droppping column")
        self.sql_type = None

    def modify_df(self, df):
        if self.sql_type is None:
            df = df.drop(columns=[self.id])
        if isinstance(self.sql_type, sqlalchemy.Date) or isinstance(
            self.sql_type, sqlalchemy.DateTime
        ):
            df[self.id] = pd.to_datetime(df[self.id], unit="s")
        if self.multiple:
            # grist uses a specific format for lists:
            # [a, b] is represented by ["L", "a", "b"].
            # we remove the leading "L"
            df[self.id] = df[self.id].map(lambda x: x[1:] if x else x)
        if isinstance(self.sql_type, sqlalchemy.Text):
            # for the columns of type text or any,
            # we serialize the object when it's a list of a dict.
            df[self.id] = df[self.id].map(
                lambda x: json.dumps(x)
                if isinstance(x, list) or isinstance(x, dict)
                else x
            )
        return df
