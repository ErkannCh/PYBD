Gab pour le lancer tu as un fichier launch.sh à la racine.
Tu le lances, tu attends genre 45 secondes et c'est bon (manip à refaire à chaque fois que tu veux voir une modif)

Le front est sur http://localhost:8050/

Les fichiers elt de traitement des données sont dans docker/elt/elt/elt.py
Pour les pages du front tu peux aller dans docker/dashboard/dashboard/tabs/tab1.py

---------------------------

A SAVIOR :
j'ai ajouté un .gitignore qui push pas le docker-compose (ça devrait eviter le merge conflict au pull mais pas sur)
Fait attention au nombre de jour que tu selectionnes dans le main pour bourso, 1 jour = 1 tranche de 10 minutes x 6 x 8 fichiers
Tu peux appeler euronext seul dans le main mais pas bourso (parce que à l'appel de euronext ça rempli la table companies)

Actuellement il y a (avec des gros guillemets):
 - l'affichage par jour qui "fonctionne"
 - l'affichage par 10 minutes qui "fonctionne" 

Niveau table j'ai rempli :
 - companies
 - daystocks
 - stocks

PROBLEMES QUE JE VOIS :
Les données ne sont pas bien formattés, il y a des doublons et autre ... (exemple va voir le jour 2020-04-01 de euronext et dans les données regarde les dates, c'est pas les bonnes)
Essaye de selectionner un seul jour dans le main pour euronext, je sais pas pourquoi il y a deux valeurs qui apparaissent sur le graph
J'ai fractionné sur le front par jour ou par 10 minutes mais l'action a gauche est dirigé par euronext, donc si tu switch sur 10 minutes et que tu vois r c'est normal, chnage d'action et tu verras quelque chose.
Y'a l'onglet graph à faire

