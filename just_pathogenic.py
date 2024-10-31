from oakvar import BasePostAggregator
from pathlib import Path
import sqlite3


class CravatPostAggregator(BasePostAggregator):
    def check(self):
        return True

    def setup(self):
        self.result_path = Path(self.output_dir, self.run_name + "_pathogenic.sqlite")
        self.filtered_conn = sqlite3.connect(self.result_path)
        self.filtered_cursor = self.filtered_conn.cursor()

        sql_create = """CREATE TABLE IF NOT EXISTS pathogenic (
            id integer NOT NULL PRIMARY KEY,
            gene text,
            rsid text,
            cdnachange text,
            genotype text,
            sequence_ontology text,
            cadd_phred float,
            sift_pred text,
            revel_score float,
            clinpred_score float,
            clinvar_sig text,
            omim_id text,
            clinvar_id text,
            ncbi_desc text
        )"""
        self.filtered_cursor.execute(sql_create)
        self.filtered_cursor.execute("DELETE FROM pathogenic;")
        self.filtered_conn.commit()

    def cleanup(self):
        if self.filtered_cursor is not None:
            self.filtered_cursor.close()
        if self.filtered_conn is not None:
            self.filtered_conn.commit()
            self.filtered_conn.close()
        return

    def annotate(self, input_data):
        # First filter group (OR conditions)
        passes_first_filter = False

        cadd_phred = input_data.get('cadd_exome__phred')
        if cadd_phred is not None and float(cadd_phred) > 10:
            passes_first_filter = True

        #sift_pred = input_data.get('sift__prediction')
        #if sift_pred == "Damaging":
        #    passes_first_filter = True

        revel_score = input_data.get('revel__score')
        if revel_score is not None and float(revel_score) > 0.5:
            passes_first_filter = True

        clinvar_sig = input_data.get('clinvar__sig')
        if clinvar_sig == "Pathogenic":
            passes_first_filter = True

        clinpred_score = input_data.get('clinpred__score')
        if clinpred_score is not None and float(clinpred_score) > 0.5:
            passes_first_filter = True

        # Second filter group (OR conditions)
        passes_second_filter = False

        omim_id = input_data.get('omim__omim_id')
        if omim_id is not None and omim_id != '':
            passes_second_filter = True

        clinvar_id = input_data.get('clinvar__id')
        if clinvar_id is not None and clinvar_id != '':
            passes_second_filter = True

        ncbi_desc = input_data.get('ncbigene__ncbi_desc')
        if ncbi_desc is not None and ncbi_desc != '':
            passes_second_filter = True

        # Both filter groups must pass (AND condition)
        if not (passes_first_filter and passes_second_filter):
            return

        sql = """INSERT INTO pathogenic (
            gene,
            cdnachange,
            genotype,
            sequence_ontology,
            cadd_phred,
            revel_score,
            clinpred_score,
            clinvar_sig,
            omim_id,
            clinvar_id,
            ncbi_desc
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)"""

        task = (
            input_data['base__hugo'],
            #input_data['dbsnp__rsid'],
            input_data['base__cchange'],
            f"{input_data['base__alt_base']}/{input_data['base__ref_base']}",
            input_data['base__so'],
            cadd_phred,
            revel_score,
            clinpred_score,
            clinvar_sig,
            omim_id,
            clinvar_id,
            ncbi_desc
        )

        self.filtered_cursor.execute(sql, task)