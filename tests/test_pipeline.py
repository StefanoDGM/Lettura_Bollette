import unittest
from decimal import Decimal
from pathlib import Path
from src.extractor.pdf_extractor import MAX_PAGES
from src.pipeline.process_bolletta import (
    VAT_REPARSE_NOTE,
    enrich_extracted_rows,
    extract_vat_imponibile_from_text,
    filter_financial_accounting_rows,
    find_detail_imponibile_issues,
    find_pdf_files,
    mark_issues_as_missing_detail,
    normalize_export_dataframe,
    normalize_detail_flags_from_rows,
    prepare_export_dataframe,
    process_all_pdfs,
    reconcile_standard_month_with_vat_summary,
    recheck_rows_if_needed,
    resolve_input_dir,
    should_prefer_detail_over_imponibile,
    supplement_summary_macro_rows,
)
import pandas as pd

class TestPipeline(unittest.TestCase):
    def test_process_empty_dir(self):
        # Test with empty directory
        empty_dir = Path("tests/empty")
        empty_dir.mkdir(exist_ok=True)
        result = process_all_pdfs(empty_dir, "*.pdf", "test.csv", "test.xlsx")
        self.assertIsNone(result)

    def test_resolve_default_input_dir(self):
        input_dir, searched_paths = resolve_input_dir(None)
        self.assertTrue(input_dir.exists())
        self.assertIn(input_dir, searched_paths)

    def test_find_pdf_files_is_case_insensitive(self):
        pdfs = find_pdf_files(Path("tests/data"), "*.pdf")
        self.assertTrue(pdfs)
        self.assertTrue(all(pdf.suffix.lower() == ".pdf" for pdf in pdfs))

    def test_normalize_export_dataframe_uses_comma_for_imports(self):
        df = pd.DataFrame(
            [
                {"importo": "123,45", "imponibile_mese": "456.78", "consumo_totale": "1000,5"},
                {"importo": "67.89", "imponibile_mese": "10,20", "consumo_totale": "2000.0"},
            ]
        )

        normalized = normalize_export_dataframe(df)

        self.assertEqual(normalized.loc[0, "importo"], "123,45")
        self.assertEqual(normalized.loc[1, "importo"], "67,89")
        self.assertEqual(normalized.loc[0, "imponibile_mese"], "456,78")
        self.assertEqual(normalized.loc[1, "imponibile_mese"], "10,2")
        self.assertEqual(normalized.loc[0, "consumo_totale"], "1000,5")
        self.assertEqual(normalized.loc[1, "consumo_totale"], "2000")

    def test_prepare_export_dataframe_prioritizes_public_logic_columns(self):
        df = pd.DataFrame(
            [
                {
                    "importo": "10.00",
                    "tipo_componente": "variabile",
                    "riferimento_ricalcolo_da": "01/01/2024",
                    "_source_file": "gennaio.pdf",
                    "categoria_parser": "evento_ricalcolo",
                    "dettaglio_ricostruzione_presente": "si",
                }
            ]
        )

        prepared = prepare_export_dataframe(df)

        self.assertEqual(
            list(prepared.columns[:4]),
            ["_source_file", "tipo_componente", "riferimento_ricalcolo_da", "importo"],
        )
        self.assertNotIn("categoria_parser", prepared.columns)
        self.assertNotIn("dettaglio_ricostruzione_presente", prepared.columns)

    def test_pdf_limit_is_twelve_pages(self):
        self.assertEqual(MAX_PAGES, 12)

    def test_extract_vat_imponibile_from_text_reads_riepilogo_iva(self):
        pdf_text = """
        FATTURA GAS AGOSTO 2023
        Riepilogo IVA
        Iva al 5%  Imponibile 1.030,21  Imposta 51,51
        """

        imponibile = extract_vat_imponibile_from_text(pdf_text)

        self.assertEqual(imponibile, Decimal("1030.21"))

    def test_extract_vat_imponibile_from_text_reads_totale_imponibile(self):
        pdf_text = """
        Riepilogo Imponibile e IVA
        Totale IMPONIBILE: 2.931,99
        Esclusi Art. 15 su 22,42
        IVA vendite 22% su 2.909,57
        """

        imponibile = extract_vat_imponibile_from_text(pdf_text)

        self.assertEqual(imponibile, Decimal("2931.99"))

    def test_extract_vat_imponibile_from_text_reads_iva_22_with_art15(self):
        pdf_text = """
        RIEPILOGO IVA
        Iva Esclusa Art.15 DPR 633/72 (su imponibile di € 130,86) € 0,00
        IVA 22% (su imponibile di € 25615,61) € 5.635,43
        """

        imponibile = extract_vat_imponibile_from_text(pdf_text)

        self.assertEqual(imponibile, Decimal("25615.61"))

    def test_extract_vat_imponibile_from_text_sums_multiple_vat_bases_for_single_month_document(self):
        pdf_text = """
        DETTAGLIO FISCALE DI QUESTA BOLLETTA
        IVA 10% su imponibile di euro 510,07
        IVA 22% su imponibile di euro 3.107,90
        """

        imponibile = extract_vat_imponibile_from_text(pdf_text)

        self.assertEqual(imponibile, Decimal("3617.97"))

    def test_supplement_summary_macro_rows_skips_generic_altre_partite_without_energy_detail(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "3845.22",
                "imponibile_mese": "6243.50",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "tipo_componente": "variabile",
                "consumo_dettaglio_riga": "",
                "quantita": "",
                "prezzo_aliquota": "",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "2085.22",
                "imponibile_mese": "6243.50",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per il trasporto e la gestione del contatore",
                "tipo_componente": "trasporto",
                "consumo_dettaglio_riga": "",
                "quantita": "",
                "prezzo_aliquota": "",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "238.53",
                "imponibile_mese": "6243.50",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per oneri di sistema",
                "tipo_componente": "oneri",
                "consumo_dettaglio_riga": "",
                "quantita": "",
                "prezzo_aliquota": "",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "74.53",
                "imponibile_mese": "6243.50",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Imposte",
                "tipo_componente": "imposte",
                "consumo_dettaglio_riga": "",
                "quantita": "",
                "prezzo_aliquota": "",
                "note": "",
            },
            {
                "data_inizio": "01/11/2023",
                "data_fine": "30/11/2023",
                "importo": "14.29",
                "imponibile_mese": "14.29",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Ricalcoli per rettifica adeguamento PCS",
                "tipo_componente": "ricalcolo_aggregato",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/11/2023",
                "riferimento_ricalcolo_a": "30/11/2023",
                "consumo_dettaglio_riga": "",
                "quantita": "",
                "prezzo_aliquota": "",
                "note": "",
            },
        ]
        pdf_text = """
        Sintesi degli importi fatturati Gas Naturale
        Spesa per la materia gas naturale € 3.845,22
        Spesa per il trasporto e la gestione del contatore € 2.085,22
        Spesa per oneri di sistema € 238,53
        Altre partite € 9,90
        Imposte € 74,53
        Ricalcoli € 14,29
        Totale Servizi Gas Naturale € 6.267,69
        """

        adjusted = supplement_summary_macro_rows(
            Path("sample.pdf"),
            rows,
            pdf_text=pdf_text,
        )

        altre_partite_rows = [row for row in adjusted if row["dettaglio_voce"] == "Altre partite"]
        self.assertEqual(len(altre_partite_rows), 0)

    def test_supplement_summary_macro_rows_adds_altre_partite_when_context_is_clearly_energetic(self):
        rows = [
            {
                "data_inizio": "01/05/2024",
                "data_fine": "31/05/2024",
                "importo": "100.00",
                "imponibile_mese": "109.90",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Materia energia",
                "tipo_componente": "variabile",
                "consumo_dettaglio_riga": "",
                "quantita": "1000",
                "prezzo_aliquota": "0.1",
                "note": "",
            }
        ]
        pdf_text = """
        Sintesi degli importi fatturati energia elettrica
        Altre partite € 9,90
        Dettaglio altre partite: corrispettivo perequativo energia del mese 01/05/2024-31/05/2024 1000 kWh a 0,0099 €/kWh
        """

        adjusted = supplement_summary_macro_rows(
            Path("sample.pdf"),
            rows,
            pdf_text=pdf_text,
        )

        altre_partite_rows = [row for row in adjusted if row["dettaglio_voce"] == "Altre partite"]
        self.assertEqual(len(altre_partite_rows), 1)
        self.assertEqual(altre_partite_rows[0]["importo"], "9.9")
        self.assertEqual(altre_partite_rows[0]["data_fine"], "31/05/2024")

    def test_filter_financial_accounting_rows_removes_anticipo_fornitura_without_energy_detail(self):
        rows = [
            {
                "data_inizio": "01/05/2024",
                "data_fine": "31/05/2024",
                "dettaglio_voce": "Materia energia",
                "tipo_componente": "variabile",
                "quantita": "1000",
                "unita_misura": "kWh",
                "prezzo_aliquota": "0.10",
                "importo": "100.00",
                "note": "",
            },
            {
                "data_inizio": "01/05/2024",
                "data_fine": "31/05/2024",
                "dettaglio_voce": "Anticipo fornitura E.E. Maggio 2024",
                "tipo_componente": "altro",
                "quantita": "",
                "unita_misura": "",
                "prezzo_aliquota": "",
                "importo": "-65573.77",
                "note": "Riepilogo oneri diversi - altre partite",
            },
        ]

        filtered = filter_financial_accounting_rows(rows)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["dettaglio_voce"], "Materia energia")

    def test_filter_financial_accounting_rows_keeps_energetic_acconto_rows(self):
        rows = [
            {
                "data_inizio": "01/05/2024",
                "data_fine": "31/05/2024",
                "dettaglio_voce": "Acconto Maggio 2024 - Materia energia",
                "tipo_componente": "variabile",
                "quantita": "1000",
                "unita_misura": "kWh",
                "prezzo_aliquota": "0.10",
                "importo": "100.00",
                "note": "",
            }
        ]

        filtered = filter_financial_accounting_rows(rows)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["dettaglio_voce"], "Acconto Maggio 2024 - Materia energia")

    def test_normalize_detail_flags_from_rows_marks_reconstructible_period_as_no(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "3845.22",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per la materia gas naturale",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "2085.22",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per il trasporto e la gestione del contatore",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "238.53",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Spesa per oneri di sistema",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "74.53",
                "manca_dettaglio": "si",
                "dettaglio_voce": "Imposte",
            },
        ]

        adjusted = normalize_detail_flags_from_rows(rows)

        self.assertTrue(all(row["manca_dettaglio"] == "no" for row in adjusted))

    def test_enrich_extracted_rows_marks_only_recalculated_row(self):
        rows = [
            {
                "_source_file": "bolletta_marzo.pdf",
                "data_inizio": "01/03/2024",
                "data_fine": "31/03/2024",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "importo": "100.00",
                "manca_dettaglio": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "categoria_parser": "",
            },
            {
                "_source_file": "bolletta_marzo.pdf",
                "data_inizio": "01/03/2024",
                "data_fine": "31/03/2024",
                "dettaglio_voce": "Spesa per il trasporto e la gestione contatore",
                "importo": "20.00",
                "manca_dettaglio": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "categoria_parser": "",
            },
            {
                "_source_file": "bolletta_marzo.pdf",
                "data_inizio": "01/02/2024",
                "data_fine": "29/02/2024",
                "dettaglio_voce": "Ricalcoli per aggiornamento componenti tariffarie",
                "importo": "5.50",
                "manca_dettaglio": "no",
                "riferimento_ricalcolo_da": "01/02/2024",
                "riferimento_ricalcolo_a": "29/02/2024",
                "categoria_parser": "",
            },
        ]

        enriched = enrich_extracted_rows(rows)

        self.assertEqual(enriched[0]["presenza_ricalcolo"], "no")
        self.assertEqual(enriched[1]["presenza_ricalcolo"], "no")
        self.assertEqual(enriched[2]["presenza_ricalcolo"], "si")
        self.assertEqual(enriched[0]["ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(enriched[1]["ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(enriched[2]["ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(enriched[0]["tipo_ricalcolo"], "")
        self.assertEqual(enriched[1]["tipo_ricalcolo"], "")
        self.assertEqual(enriched[2]["tipo_ricalcolo"], "importo")
        self.assertTrue(
            all(row["totale_documento_puo_non_coincidere_con_mese_corrente"] == "si" for row in enriched)
        )

    def test_enrich_extracted_rows_preserves_true_multi_month_aggregate(self):
        rows = [
            {
                "_source_file": "bolletta_dicembre.pdf",
                "data_inizio": "01/12/2024",
                "data_fine": "31/12/2024",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "importo": "1000.00",
                "manca_dettaglio": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "categoria_parser": "",
            },
            {
                "_source_file": "bolletta_dicembre.pdf",
                "data_inizio": "01/09/2024",
                "data_fine": "30/11/2024",
                "dettaglio_voce": "Ricalcoli per aggiornamento componenti tariffarie",
                "importo": "90.00",
                "manca_dettaglio": "no",
                "riferimento_ricalcolo_da": "01/09/2024",
                "riferimento_ricalcolo_a": "30/11/2024",
                "categoria_parser": "",
            },
        ]

        enriched = enrich_extracted_rows(rows)

        self.assertEqual(enriched[0]["presenza_ricalcolo"], "no")
        self.assertEqual(enriched[0]["ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(enriched[1]["presenza_ricalcolo"], "si")
        self.assertEqual(enriched[1]["ricalcolo_aggregato_multi_mese"], "si")
        self.assertEqual(enriched[1]["tipo_ricalcolo"], "importo")
        self.assertEqual(enriched[1]["categoria_parser"], "totale_aggregato_multi_mese")
        self.assertTrue(
            all(row["totale_documento_puo_non_coincidere_con_mese_corrente"] == "si" for row in enriched)
        )

    def test_enrich_extracted_rows_does_not_treat_generic_conguaglio_wording_as_recalculation(self):
        rows = [
            {
                "_source_file": "bolletta_luglio.pdf",
                "data_inizio": "01/07/2024",
                "data_fine": "31/07/2024",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "importo": "100.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "note": "Totale bolletta (con riserva di conguaglio)",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "categoria_parser": "",
            }
        ]

        enriched = enrich_extracted_rows(rows)

        self.assertEqual(enriched[0]["presenza_ricalcolo"], "no")
        self.assertEqual(enriched[0]["ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(enriched[0]["tipo_ricalcolo"], "")

    def test_reconcile_standard_month_with_vat_summary_replaces_misread_imponibile(self):
        rows = [
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "importo": "709.79",
                "imponibile_mese": "1007.49",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "importo": "279.15",
                "imponibile_mese": "1007.49",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "importo": "17.84",
                "imponibile_mese": "1007.49",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "importo": "15.83",
                "imponibile_mese": "1007.49",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "importo": "7.60",
                "imponibile_mese": "1007.49",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
        ]

        adjusted = reconcile_standard_month_with_vat_summary(
            Path("sample.pdf"),
            rows,
            vat_reader=lambda _pdf_path: Decimal("1030.21"),
        )

        self.assertTrue(all(row["imponibile_mese"] == "1030.21" for row in adjusted))
        self.assertTrue(all(row["manca_dettaglio"] == "no" for row in adjusted))
        self.assertIn(VAT_REPARSE_NOTE, adjusted[0]["note"])

    def test_reconcile_standard_month_with_vat_summary_marks_art15_even_when_imponibile_is_already_correct(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "16930.32",
                "imponibile_mese": "25615.61",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "2081.88",
                "imponibile_mese": "25615.61",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "5151.61",
                "imponibile_mese": "25615.61",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "135.86",
                "imponibile_mese": "25615.61",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "1446.80",
                "imponibile_mese": "25615.61",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
        ]

        def fake_vat_reader(_pdf_path):
            return Decimal("25615.61")

        from unittest.mock import patch

        pdf_text = """
        RIEPILOGO IVA
        Iva Esclusa Art.15 DPR 633/72 (su imponibile di € 130,86) € 0,00
        IVA 22% (su imponibile di € 25615,61) € 5.635,43
        """

        with patch("src.pipeline.process_bolletta.extract_text_from_pdf", return_value=pdf_text):
            adjusted = reconcile_standard_month_with_vat_summary(
                Path("sample.pdf"),
                rows,
                vat_reader=fake_vat_reader,
            )

        self.assertTrue(all(row["imponibile_mese"] == "25615.61" for row in adjusted))
        self.assertIn("art15_excluded_in_iva_summary", adjusted[0]["note"])

    def test_reconcile_standard_month_with_vat_summary_uses_sum_of_multiple_vat_bases(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "2952.54",
                "imponibile_mese": "3107.90",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "665.43",
                "imponibile_mese": "3107.90",
                "manca_dettaglio": "no",
                "presenza_ricalcolo": "no",
                "note": "",
            },
        ]

        from unittest.mock import patch

        pdf_text = """
        Dettaglio fiscale di questa bolletta
        IVA 10% su imponibile di euro 510,07
        IVA 22% su imponibile di euro 3.107,90
        """

        with patch("src.pipeline.process_bolletta.extract_text_from_pdf", return_value=pdf_text):
            adjusted = reconcile_standard_month_with_vat_summary(Path("sample.pdf"), rows)

        self.assertTrue(all(row["imponibile_mese"] == "3617.97" for row in adjusted))
        self.assertTrue(all(row["manca_dettaglio"] == "no" for row in adjusted))
        self.assertIn(VAT_REPARSE_NOTE, adjusted[0]["note"])

    def test_find_detail_imponibile_issues_when_detail_does_not_match_single_period(self):
        rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "50.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
        ]

        issues = find_detail_imponibile_issues(rows)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["data_fine"], "31/07/2025")
        self.assertEqual(issues[0]["sum_importo"], "150")
        self.assertEqual(issues[0]["imponibile_mese"], "110")

    def test_find_detail_imponibile_issues_detects_mismatch_even_with_other_periods(self):
        rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/08/2025",
                "data_fine": "31/08/2025",
                "importo": "50.00",
                "imponibile_mese": "50.00",
                "manca_dettaglio": "no",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
        ]

        issues = find_detail_imponibile_issues(rows)

        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["cross_period_context"], "si")
        self.assertEqual(issues[0]["preferred_detail_source"], "full_sum")

    def test_mark_issues_as_missing_detail_sets_flag_and_note(self):
        rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "note": "",
            }
        ]
        issues = [{"data_inizio": "01/07/2025", "data_fine": "31/07/2025", "imponibile_mese": "150"}]

        adjusted = mark_issues_as_missing_detail(rows, issues)

        self.assertEqual(adjusted[0]["manca_dettaglio"], "si")
        self.assertEqual(adjusted[0]["imponibile_mese"], "150")
        self.assertIn("Dettaglio non coerente con imponibile del periodo dopo seconda verifica", adjusted[0]["note"])

    def test_recheck_rows_if_needed_keeps_detail_flag_no_and_adds_warning_when_no_recalc_mismatch_remains(self):
        rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "50.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
        ]

        def fake_review(_pdf_path, _model, _issues):
            return rows

        adjusted = recheck_rows_if_needed(Path("sample.pdf"), rows, "gpt-5", fake_review)

        self.assertEqual(adjusted[0]["manca_dettaglio"], "no")
        self.assertEqual(adjusted[1]["manca_dettaglio"], "no")
        self.assertIn(
            "Nonostante non ci siano ricalcoli, somma dettaglio e imponibile del documento sono diversi",
            adjusted[0]["note"],
        )

    def test_recheck_rows_if_needed_uses_reviewed_rows_if_consistency_improves(self):
        original_rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "50.00",
                "imponibile_mese": "110.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
        ]
        reviewed_rows = [
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "100.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/07/2025",
                "data_fine": "31/07/2025",
                "importo": "50.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
        ]

        adjusted = recheck_rows_if_needed(
            Path("sample.pdf"),
            original_rows,
            "gpt-5",
            lambda _pdf_path, _model, _issues: reviewed_rows,
        )

        self.assertEqual(adjusted[0]["manca_dettaglio"], "no")
        self.assertEqual(adjusted[0]["imponibile_mese"], "150.00")
        self.assertNotIn("imponibile_mese_validato", adjusted[0])

    def test_recheck_rows_if_needed_keeps_document_imponibile_when_no_recalc_and_detail_mismatch_remains(self):
        rows = [
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "8560.43",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Quota variabile",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "15.00",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Quota fissa",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "94.99",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Trasporto quota fissa",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "2714.85",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Trasporto quota energia",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "-2.18",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Oneri quota fissa",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "183.39",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Oneri quota energia",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "2158.90",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Accisa",
            },
            {
                "data_inizio": "01/04/2023",
                "data_fine": "30/04/2023",
                "importo": "359.70",
                "imponibile_mese": "14531.38",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Addizionale enti locali",
            },
        ]

        issues = find_detail_imponibile_issues(rows)
        self.assertFalse(should_prefer_detail_over_imponibile(rows, issues[0]))

        adjusted = recheck_rows_if_needed(Path("sample.pdf"), rows, "gpt-5", lambda _pdf_path, _model, _issues: rows)

        self.assertEqual(len(issues), 1)
        self.assertEqual(adjusted[0]["manca_dettaglio"], "no")
        self.assertEqual(adjusted[0]["imponibile_mese"], "14531.38")
        self.assertIn(
            "Nonostante non ci siano ricalcoli, somma dettaglio e imponibile del documento sono diversi",
            adjusted[0]["note"],
        )

    def test_recheck_rows_if_needed_prefers_month_detail_and_relocates_explicit_recalc_rows(self):
        rows = [
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "importo": "80.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Quota energia",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "importo": "50.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Trasporto",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "importo": "20.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Oneri",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "importo": "-30.00",
                "imponibile_mese": "150.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/12/2022",
                "riferimento_ricalcolo_a": "31/12/2022",
                "dettaglio_voce": "Ricalcolo dicembre",
            },
            {
                "data_inizio": "01/02/2023",
                "data_fine": "28/02/2023",
                "importo": "25.00",
                "imponibile_mese": "25.00",
                "manca_dettaglio": "no",
                "note": "",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
                "dettaglio_voce": "Altro mese",
            },
        ]

        issues = find_detail_imponibile_issues(rows)
        january_issue = issues[0]
        january_rows = [row for row in rows if row["data_fine"] == "31/01/2023"]

        self.assertEqual(january_issue["preferred_detail_source"], "base_sum_without_explicit_recalc")
        self.assertTrue(should_prefer_detail_over_imponibile(january_rows, january_issue))

        adjusted = recheck_rows_if_needed(Path("sample.pdf"), rows, "gpt-5", lambda _pdf_path, _model, _issues: rows)

        january_adjusted = [row for row in adjusted if row["data_fine"] == "31/01/2023"]
        self.assertEqual(len(january_adjusted), 3)
        self.assertTrue(all(row["imponibile_mese"] == "150.00" for row in january_adjusted))
        self.assertTrue(all(row["manca_dettaglio"] == "no" for row in january_adjusted))

        relocated = [row for row in adjusted if row["dettaglio_voce"] == "Ricalcolo dicembre"][0]
        self.assertEqual(relocated["data_inizio"], "01/12/2022")
        self.assertEqual(relocated["data_fine"], "31/12/2022")
        self.assertIn("Riga di ricalcolo esclusa dal dettaglio del mese", relocated["note"])

if __name__ == "__main__":
    unittest.main()
