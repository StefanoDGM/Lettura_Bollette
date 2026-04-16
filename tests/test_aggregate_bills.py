import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.pipeline.aggregate_bills import (
    INPUT_PATH,
    OUT_XLSX,
    aggregate_bolletta_data,
    resolve_input_path,
    resolve_output_path,
)


class TestAggregateBills(unittest.TestCase):
    def test_resolve_default_input_path(self):
        input_path, searched_paths = resolve_input_path(None)
        self.assertEqual(input_path.name, INPUT_PATH.name)
        self.assertIn(input_path, searched_paths)

    def test_resolve_default_output_path(self):
        output_path = resolve_output_path(None)
        self.assertEqual(output_path, OUT_XLSX)
        self.assertEqual(output_path.parent, INPUT_PATH.parent)

    def test_standard_month_uses_document_imponibile_without_recalculation(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "6.00",
                "imponibile_mese": "10.00",
                "consumo_totale": "123.4",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Quota energia",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "4.00",
                "imponibile_mese": "10.00",
                "consumo_totale": "123.4",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Trasporto",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 10.0)
        self.assertEqual(agg.loc[0, "importo_logica_usata"], "imponibile_documento")
        self.assertEqual(agg.loc[0, "nessun_ricalcolo_rilevato_imponibile_usato_come_fonte_finale"], "si")
        self.assertEqual(agg.loc[0, "ricalcolo_presente"], "no")
        self.assertEqual(agg.loc[0, "consumo_mese_ricostruibile"], "si")
        self.assertEqual(agg.loc[0, "importo_mese_ricostruibile"], "si")
        self.assertEqual(int(agg.loc[0, "confidenza_percent"]), 100)

    def test_non_aggregated_recalculation_uses_month_detail(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "100.00",
                "imponibile_mese": "100.00",
                "consumo_totale": "1000",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Fornitura gennaio",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "-20.00",
                "imponibile_mese": "-20.00",
                "consumo_totale": "900",
                "consumo_dettaglio_riga": "-100",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "marzo_2024.pdf",
                "dettaglio_voce": "Ricalcolo gennaio",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 80.0)
        self.assertEqual(agg.loc[0, "ricalcolo_presente"], "si")
        self.assertEqual(agg.loc[0, "ricalcolo_aggregato_multi_mese"], "no")
        self.assertEqual(agg.loc[0, "totale_documento_non_confrontabile_direttamente_con_mese_corrente"], "si")
        self.assertLess(int(agg.loc[0, "confidenza_percent"]), 100)

    def test_month_uses_base_imponibile_plus_later_recalculation_rows(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "60.00",
                "imponibile_mese": "100.00",
                "consumo_totale": "1000",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Quota energia",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "30.00",
                "imponibile_mese": "100.00",
                "consumo_totale": "1000",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Trasporto",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "5.00",
                "imponibile_mese": "100.00",
                "consumo_totale": "1000",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Imposte",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "-10.00",
                "imponibile_mese": "-10.00",
                "consumo_totale": "900",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "febbraio_2024.pdf",
                "dettaglio_voce": "Ricalcolo gennaio",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        gennaio = agg.loc[(agg["anno"] == 2024) & (agg["mese_num"] == 1)].iloc[0]
        self.assertAlmostEqual(float(gennaio["totale_importi"]), 90.0)
        self.assertEqual(gennaio["importo_logica_usata"], "imponibile_documento_piu_rettifiche_nel_mese")
        self.assertIn(
            "Per questo mese sono presenti ricalcoli anche in altre bollette: usato l'imponibile della bolletta base e sommate le rettifiche successive",
            gennaio["warning_mese"],
        )
        self.assertIn("rettificato da una bolletta successiva", gennaio["confidenza_motivo"].lower())

    def test_month_uses_detail_sum_when_document_contains_recalc_for_other_month(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "3845.22",
                "imponibile_mese": "6243.50",
                "consumo_totale": "5963",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Spesa per la materia gas naturale",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "2085.22",
                "imponibile_mese": "6243.50",
                "consumo_totale": "5963",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Spesa per il trasporto e la gestione del contatore",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "238.53",
                "imponibile_mese": "6243.50",
                "consumo_totale": "5963",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Spesa per oneri di sistema",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "74.53",
                "imponibile_mese": "6243.50",
                "consumo_totale": "5963",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Imposte (accise erariali)",
            },
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "9.90",
                "imponibile_mese": "6243.50",
                "consumo_totale": "5963",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Altre partite",
            },
            {
                "data_inizio": "01/11/2023",
                "data_fine": "30/11/2023",
                "importo": "14.29",
                "imponibile_mese": "14.29",
                "consumo_totale": "3227",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "2024_1_Gennaio.pdf",
                "dettaglio_voce": "Ricalcoli per rettifica adeguamento PCS",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/11/2023",
                "riferimento_ricalcolo_a": "30/11/2023",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        gennaio = agg.loc[(agg["anno"] == 2024) & (agg["mese_num"] == 1)].iloc[0]
        novembre = agg.loc[(agg["anno"] == 2023) & (agg["mese_num"] == 11)].iloc[0]
        self.assertAlmostEqual(float(gennaio["totale_importi"]), 6253.40)
        self.assertEqual(gennaio["importo_logica_usata"], "dettaglio_mese_con_ricalcolo")
        self.assertIn(
            "Imponibile preso da somma del dettaglio del mese, in quanto nella bolletta erano presenti anche altri mesi come ricalcolo",
            gennaio["warning_mese"],
        )
        self.assertAlmostEqual(float(novembre["totale_importi"]), 14.29)

    def test_single_month_recalculation_with_block_flag_is_not_treated_as_aggregated_event(self):
        rows = [
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "561.10",
                "imponibile_mese": "910.08",
                "consumo_totale": "853",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_4_Aprile.pdf",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "309.72",
                "imponibile_mese": "910.08",
                "consumo_totale": "853",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_4_Aprile.pdf",
                "dettaglio_voce": "Spesa per il trasporto e la gestione del contatore",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "28.60",
                "imponibile_mese": "910.08",
                "consumo_totale": "853",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_4_Aprile.pdf",
                "dettaglio_voce": "Spesa per oneri di sistema",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "10.66",
                "imponibile_mese": "910.08",
                "consumo_totale": "853",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "2024_4_Aprile.pdf",
                "dettaglio_voce": "Imposta erariale",
                "unita_misura": "Smc",
                "quantita": "853",
                "prezzo_aliquota": "0.012498",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "-531.27",
                "imponibile_mese": "2004.72",
                "consumo_totale": "2979",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_storno",
                "_source_file": "2024_4_Aprile_con_ricalcolo.pdf",
                "dettaglio_voce": "Ricalcolo in detrazione",
                "tipo_componente": "ricalcolo_aggregato",
                "unita_misura": "Smc",
                "quantita": "-853",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/04/2024",
                "riferimento_ricalcolo_a": "30/04/2024",
                "manca_dettaglio_ricalcolo": "si",
                "ricalcolo_spalmabile": "no",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "1853.28",
                "imponibile_mese": "2004.72",
                "consumo_totale": "2979",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "2024_4_Aprile_con_ricalcolo.pdf",
                "dettaglio_voce": "Ricalcolo in aumento",
                "tipo_componente": "ricalcolo_aggregato",
                "unita_misura": "Smc",
                "quantita": "2979",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/04/2024",
                "riferimento_ricalcolo_a": "30/04/2024",
                "manca_dettaglio_ricalcolo": "si",
                "ricalcolo_spalmabile": "no",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)
            warning_report = agg.attrs["warning_report"]

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 2232.09)
        self.assertAlmostEqual(float(agg.loc[0, "consumo_mese"]), 2979.0)
        self.assertEqual(agg.loc[0, "consumo_logica_usata"], "consumo_totale_ultimo_ricalcolo_mese")
        self.assertEqual(agg.loc[0, "importo_logica_usata"], "imponibile_documento_piu_rettifiche_nel_mese")
        self.assertEqual(agg.loc[0, "ricalcolo_aggregato_multi_mese"], "no")
        self.assertFalse(bool(agg.loc[0, "ricalcolo_aggregato_presente"]))
        self.assertEqual(agg.loc[0, "verifica_manuale_consigliata"], "no")
        self.assertEqual(agg.loc[0, "source_file_distinti"], 2)
        april_warning = warning_report.loc[warning_report["mese"] == "aprile"].iloc[0]
        self.assertNotIn("non bastano per ricostruire il mese", april_warning["warning_mese"].lower())
        self.assertNotIn("ignorato il totale aggregato", april_warning["warning_mese"].lower())
        self.assertIn("consumo del mese corretto da ricalcolo", april_warning["warning_mese"].lower())
        self.assertIn("imponibile della bolletta base", agg.loc[0, "warning_mese"].lower())

    def test_aggregated_multi_month_with_reconstructible_detail_does_not_allocate_total(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "50.00",
                "imponibile_mese": "50.00",
                "consumo_totale": "100",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Gennaio reale",
            },
            {
                "data_inizio": "01/02/2024",
                "data_fine": "29/02/2024",
                "importo": "70.00",
                "imponibile_mese": "70.00",
                "consumo_totale": "200",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "febbraio_2024.pdf",
                "dettaglio_voce": "Febbraio reale",
            },
            {
                "data_inizio": "01/03/2024",
                "data_fine": "31/03/2024",
                "importo": "40.00",
                "imponibile_mese": "",
                "consumo_totale": "",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "totale_aggregato_multi_mese",
                "_source_file": "marzo_2024.pdf",
                "dettaglio_voce": "Ricalcolo gennaio-febbraio",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/01/2024",
                "riferimento_ricalcolo_a": "29/02/2024",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)
            warning_report = agg.attrs["warning_report"]

        self.assertEqual(len(agg), 2)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 50.0)
        self.assertAlmostEqual(float(agg.loc[1, "totale_importi"]), 70.0)
        self.assertTrue((warning_report["mese"] == "marzo").any())
        march_warning = warning_report.loc[warning_report["mese"] == "marzo"].iloc[0]
        self.assertIn("mese e ricostruibile dal dettaglio", march_warning["warning_mese"].lower())

    def test_aggregated_multi_month_without_detail_uses_divide_by_two_fallback(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "50.00",
                "imponibile_mese": "50.00",
                "consumo_totale": "100",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Gennaio reale",
            },
            {
                "data_inizio": "01/02/2024",
                "data_fine": "29/02/2024",
                "importo": "70.00",
                "imponibile_mese": "70.00",
                "consumo_totale": "200",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "febbraio_2024.pdf",
                "dettaglio_voce": "Febbraio reale",
            },
            {
                "data_inizio": "01/03/2024",
                "data_fine": "31/03/2024",
                "importo": "20.00",
                "imponibile_mese": "",
                "consumo_totale": "",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "no",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "totale_aggregato_multi_mese",
                "_source_file": "marzo_2024.pdf",
                "dettaglio_voce": "Ricalcolo gennaio-febbraio sintetico",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/01/2024",
                "riferimento_ricalcolo_a": "29/02/2024",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 2)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 60.0)
        self.assertAlmostEqual(float(agg.loc[1, "totale_importi"]), 80.0)
        self.assertEqual(agg.loc[0, "allocazione_fallback_dividi_per_due"], "si")
        self.assertEqual(agg.loc[1, "allocazione_fallback_dividi_per_due"], "si")
        self.assertLess(int(agg.loc[0, "confidenza_percent"]), 70)

    def test_aggregated_multi_month_without_detail_over_two_months_requires_manual_review(self):
        rows = [
            {
                "data_inizio": "01/01/2024",
                "data_fine": "31/01/2024",
                "importo": "50.00",
                "imponibile_mese": "50.00",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "gennaio_2024.pdf",
                "dettaglio_voce": "Gennaio reale",
            },
            {
                "data_inizio": "01/04/2024",
                "data_fine": "30/04/2024",
                "importo": "30.00",
                "imponibile_mese": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "no",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "totale_aggregato_multi_mese",
                "_source_file": "aprile_2024.pdf",
                "dettaglio_voce": "Ricalcolo gennaio-marzo sintetico",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/01/2024",
                "riferimento_ricalcolo_a": "31/03/2024",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)
            warning_report = agg.attrs["warning_report"]

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), 50.0)
        april_warning = warning_report.loc[warning_report["mese"] == "aprile"].iloc[0]
        self.assertEqual(april_warning["verifica_manuale_consigliata"], "si")
        self.assertIn("non allocato automaticamente", april_warning["warning_mese"].lower())

    def test_infer_a2a_components_does_not_fall_back_to_non_classifiable(self):
        rows = [
            {
                "data_inizio": "01/07/2023",
                "data_fine": "31/07/2023",
                "importo": "31530.47",
                "imponibile_mese": "31530.47",
                "unita_misura": "",
                "dettaglio_voce": "Totale luglio",
                "tipo_componente": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "luglio_2023.pdf",
                "blocco_ricalcolo_aggregato": "no",
                "riferimento_ricalcolo_da": "",
                "riferimento_ricalcolo_a": "",
            },
            {
                "data_inizio": "01/07/2023",
                "data_fine": "31/07/2023",
                "importo": "26757.76",
                "imponibile_mese": "31530.47",
                "unita_misura": "€/Smc",
                "dettaglio_voce": "Quota Proporzionale rispetto ai consumi",
                "tipo_componente": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "settembre_2023.pdf",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/07/2023",
                "riferimento_ricalcolo_a": "31/07/2023",
            },
            {
                "data_inizio": "01/07/2023",
                "data_fine": "31/07/2023",
                "importo": "1918.80",
                "imponibile_mese": "31530.47",
                "unita_misura": "€/mese/IG",
                "dettaglio_voce": "Quota Fissa",
                "tipo_componente": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_ricalcolo",
                "_source_file": "settembre_2023.pdf",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/07/2023",
                "riferimento_ricalcolo_a": "31/07/2023",
            },
            {
                "data_inizio": "01/07/2023",
                "data_fine": "31/07/2023",
                "importo": "-28676.56",
                "imponibile_mese": "31530.47",
                "unita_misura": "",
                "dettaglio_voce": "Storno per rettifica dal 01.07.2023 al 31.07.2023",
                "tipo_componente": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "evento_storno",
                "_source_file": "settembre_2023.pdf",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/07/2023",
                "riferimento_ricalcolo_a": "31/07/2023",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)
            warning_report = agg.attrs["warning_report"]

        july_warning = warning_report.loc[warning_report["mese"] == "luglio"].iloc[0]
        self.assertNotIn("non classificabile", july_warning["warning_mese"].lower())

    def test_month_quantity_rows_override_multi_month_document_total_for_consumo(self):
        rows = [
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "consumo_totale": "74569",
                "quantita": "15037",
                "unita_misura": "Smc",
                "importo": "6756.86",
                "imponibile_mese": "9396.01",
                "dettaglio_voce": "Quota Proporzionale rispetto ai consumi",
                "tipo_componente": "variabile",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "08-2023.pdf",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "consumo_totale": "74569",
                "quantita": "15037",
                "unita_misura": "Smc",
                "importo": "287.21",
                "imponibile_mese": "9396.01",
                "dettaglio_voce": "Componente CRVBL",
                "tipo_componente": "trasporto",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "08-2023.pdf",
            },
            {
                "data_inizio": "01/08/2023",
                "data_fine": "31/08/2023",
                "consumo_totale": "74569",
                "quantita": "15037",
                "unita_misura": "Smc",
                "importo": "93.97",
                "imponibile_mese": "9396.01",
                "dettaglio_voce": "Addizionale Regionale Industriali",
                "tipo_componente": "imposte",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "no",
                "categoria_parser": "riga_analitica_mese",
                "_source_file": "08-2023.pdf",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "consumo_mese"]), 15037.0)
        self.assertEqual(agg.loc[0, "consumo_logica_usata"], "quantita_righe_periodo")

    def test_repeated_scaglione_quantities_do_not_override_single_month_total_consumption(self):
        rows = [
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "",
                "unita_misura": "",
                "importo": "28135.36",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Spesa per la materia gas naturale",
                "tipo_componente": "variabile",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "480",
                "unita_misura": "€/Smc",
                "importo": "6.48",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Componente CRVBL",
                "tipo_componente": "trasporto",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "31217",
                "unita_misura": "€/Smc",
                "importo": "421.43",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Componente CRVBL",
                "tipo_componente": "trasporto",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "480",
                "unita_misura": "€/Smc",
                "importo": "17.47",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Componente CRVOS",
                "tipo_componente": "trasporto",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "120",
                "unita_misura": "€/Smc",
                "importo": "5.28",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Imposta erariale di consumo 1° scaglione",
                "tipo_componente": "imposte",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
            {
                "data_inizio": "01/01/2023",
                "data_fine": "31/01/2023",
                "consumo_totale": "31697",
                "quantita": "26697",
                "unita_misura": "€/Smc",
                "importo": "421.81",
                "imponibile_mese": "38376.81",
                "dettaglio_voce": "Oneri di sistema - 5° scaglione",
                "tipo_componente": "oneri",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "no",
                "ricalcolo_aggregato_multi_mese": "no",
                "_source_file": "01_2023.PDF",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(float(agg.loc[0, "consumo_mese"]), 31697.0)
        self.assertEqual(agg.loc[0, "consumo_logica_usata"], "consumo_totale")

    def test_aggregated_recalculation_with_monthly_consumption_support_marks_import_as_not_reconstructible(self):
        rows = [
            {
                "data_inizio": "01/09/2023",
                "data_fine": "30/09/2023",
                "importo": "",
                "imponibile_mese": "",
                "consumo_totale": "1396",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "tabella_supporto_consumi_mensili",
                "_source_file": "settembre_dicembre_2023.pdf",
                "dettaglio_voce": "Settembre 2023 reale",
                "quantita": "100",
                "unita_misura": "Smc",
            },
            {
                "data_inizio": "01/10/2023",
                "data_fine": "31/10/2023",
                "importo": "",
                "imponibile_mese": "",
                "consumo_totale": "200",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "tabella_supporto_consumi_mensili",
                "_source_file": "settembre_dicembre_2023.pdf",
                "dettaglio_voce": "Ottobre 2023 reale",
                "quantita": "200",
                "unita_misura": "Smc",
            },
            {
                "data_inizio": "01/11/2023",
                "data_fine": "30/11/2023",
                "importo": "",
                "imponibile_mese": "",
                "consumo_totale": "300",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "no",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "tabella_supporto_consumi_mensili",
                "_source_file": "settembre_dicembre_2023.pdf",
                "dettaglio_voce": "Novembre 2023 reale",
                "quantita": "300",
                "unita_misura": "Smc",
            },
            {
                "data_inizio": "01/12/2023",
                "data_fine": "31/12/2023",
                "importo": "-60.00",
                "imponibile_mese": "",
                "consumo_totale": "600",
                "consumo_dettaglio_riga": "",
                "manca_dettaglio": "no",
                "manca_dettaglio_consumo": "si",
                "presenza_ricalcolo": "si",
                "ricalcolo_aggregato_multi_mese": "si",
                "dettaglio_ricostruzione_presente": "si",
                "totale_documento_puo_non_coincidere_con_mese_corrente": "si",
                "categoria_parser": "totale_aggregato_multi_mese",
                "_source_file": "settembre_dicembre_2023.pdf",
                "dettaglio_voce": "Ricalcoli settembre-novembre",
                "blocco_ricalcolo_aggregato": "si",
                "riferimento_ricalcolo_da": "01/09/2023",
                "riferimento_ricalcolo_a": "30/11/2023",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            input_path = tmp_dir_path / "input.csv"
            output_path = tmp_dir_path / "output.xlsx"
            pd.DataFrame(rows).to_csv(input_path, index=False)
            agg = aggregate_bolletta_data(input_path, output_path)

        self.assertEqual(len(agg), 3)
        self.assertAlmostEqual(float(agg.loc[0, "totale_importi"]), -10.0)
        self.assertAlmostEqual(float(agg.loc[1, "totale_importi"]), -20.0)
        self.assertAlmostEqual(float(agg.loc[2, "totale_importi"]), -30.0)
        self.assertTrue((agg["consumo_mese_ricostruibile"] == "si").all())
        self.assertTrue((agg["importo_mese_ricostruibile"] == "no").all())
        self.assertTrue(
            agg["warning_mese"].fillna("").str.contains("importo non verificabile", case=False, regex=False).all()
        )
        self.assertTrue(
            agg["warning_mese"].fillna("").str.contains("consumo mensile ricostruibile", case=False, regex=False).all()
        )
        self.assertTrue(
            agg["consumo_confidenza_motivo"].fillna("").str.contains("consumo", case=False, regex=False).all()
        )


if __name__ == "__main__":
    unittest.main()
