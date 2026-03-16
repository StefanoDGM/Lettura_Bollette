controllare su https://platform.openai.com/usage quanti soldi si hanno a disposizione per valutare i costi e le richieste.

Lo script fa questo:
-Una chiamata per ogni file bolletta chiedendo in output a chatgpt 5.0 un file excel con imponibile, consumi elettrico o gas, importi, data di ogni movimento relativo al dettaglio di ogni singola bolletta ecc.
-Unire tutto in un unico file per poi raggruppare per mese e ottenere la somma degli importi che dovrebbe corrispondere all'imponibile (in quanto se ci fosse un conguaglio da fare di marzo in una bolletta di aprile, ci sarebbe la data di marzo e quindi non si sommerebbe)
-In caso di mancanza di dettaglio, prende l'imponibile dalla colonna "imponibile" quindi senza sommare

Casi NON Gestiti:
Ricalcoli / Conguagli quando il dettaglio non è presente (in quanto a quel punto si limita a leggere l'imponibile contenente erroneamente anche il ricalcolo del mese passato all'interno del mese corrente)
più bollette accorpate in una unica

Come Avviare il codice:
-Caricare file nella stessa cartella del progetto
-Eseguire file.ipynb


ps. 
ricordarsi di eliminare i file in output una volta copiati altrimenti la seconda volta non riuscirà a ricreare i file, in quanto già presenti.