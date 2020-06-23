package com.stirperichard.stormbus.utils;


public class ParseTime {

    //Converto la colonna non omogenea in soli interi [minutes] <- misura std di riferimento
    public static int minutesDelayed(String delay) {

        String[] delays;
        int newDelay = 0;

        //Se è vuota o contiene "?", assegno 0 di default
        if (delay.isEmpty() || delay.equals("?") || delay.equals("!")){
            return 0;
        }

        //Cerco di rendere la colonna dei delay quanto più omogena possibile
        //Tolgo caratteri inutili

        //Ci sono dei valori che al posto di "1" contengono "I"
        if(String.valueOf(delay.charAt(0)).equals("I")){
            String subs = delay.substring(1).replaceAll("[^-ImMhH0123456789/]", "");
            delay = "1" + subs;
        } else {
            delay = delay.replaceAll("[^-mMhH0123456789/]", "");
        } //Ritorna una stringa di solo numeri, m, h e nel caso trova un I al primo posto lo converte in 1


        //Se nel primo carattere rimane il "-" lo tolgo
        if(!delay.isEmpty()){
            String first = String.valueOf(delay.charAt(0));
            if(first.equals("-")){
                delay = delay.substring(1);
            }
        }


        //Sostituisco le lettere così rendo quanto più omogeneo possibile
        delay = delay.replace("M", "m");
        delay = delay.replace("H", "h");

        //Controllo se ha "-" oppure "/" -> SPLIT
        if (delay.contains("-") || delay.contains("/")){
            delays = delay.split("[-/]", -1);

            if (delays[0].equals("1") && delays[1].contains("2")){
                delay = String.valueOf(30);
                return Integer.valueOf(delay);
            }

            int i;
            for(i = 0; i < delays.length; i++){

                //Controllo se sono ORE
                if(delays[i].contains("h")){
                    //Rimuovo le lettere
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    delays[i] = checkEmpty(delays[i]);

                    //Trasformo le ore in minuti
                    delays[i] = String.valueOf(Integer.parseInt(delays[i]) * 60);

                    if ((i-1) >= 0 && !(delays[i-1].contains("m"))) {
                        newDelay = newDelay / 2;
                        int del = Integer.parseInt(delays[i]);
                        newDelay += del / 2;
                    } else {
                        newDelay += Integer.parseInt(delays[i]);
                    }

                } else if (delays[i].contains("m")){

                    //Controllo se sono minuti
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    delays[i] = checkEmpty(delays[i]);
                    int del = Integer.parseInt(delays[i]);
                    if ((i-1) >= 0 && !(delays[i-1].contains("h"))) {
                        del = del / 2;
                        newDelay += del;
                    } else {
                        newDelay += del;
                    }

                } else {

                    //Controllo se non ha lettere, assegno di default minuti
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    delays[i] = checkEmpty(delays[i]);

                    newDelay += (Integer.parseInt(delays[i])/2);
                }
            }
        } else{

            //CASO IN CUI NON ABBIA CARATTERI DI SPLIT e quindi siano scritti consecutivi

            if (delay.contains("h") && delay.contains("m")){
                String [] delaysS;

                //Faccio split in base alla lettera che viene prima
                if(delay.indexOf("h") < delay.indexOf("m")){
                    delaysS = delay.split("h");
                } else {
                    delaysS = delay.split("m");
                }

                for (int j = 0; j < delaysS.length; j++){
                    if (j== 0){
                        delaysS[j] = checkEmpty(delaysS[j]);
                        newDelay = newDelay + (Integer.parseInt((String.valueOf(delaysS[j])))*60);
                    } else {
                        delaysS[j] = delaysS[j].replaceAll("[^0-9]+", "");
                        delaysS[j] = checkEmpty(delaysS[j]);
                        newDelay += Integer.parseInt(String.valueOf(delaysS[j]));
                    }
                }

            }
            //Se non contiene unità di misura -> minuti
            if (!(delay.contains("h")  || delay.contains("m"))){
                delay = delay.replaceAll("[^0-9]+", "");
                delay = checkEmpty(delay);
                newDelay = Integer.parseInt(delay);
            }
            //Se contiene solo ORE
            if (delay.contains("h") && !(delay.contains("m"))){
                delay = delay.replaceAll("[^0-9]+", "");
                delay = checkEmpty(delay);
                newDelay = Integer.parseInt(delay);
                newDelay *= 60;
            }
            //Se contiene solo MINUTI
            if (!(delay.contains("h")) && delay.contains("m")){
                delay = delay.replaceAll("[^0-9]+", "");
                delay = checkEmpty(delay);
                newDelay = Integer.parseInt(delay);
            }
        }

        return newDelay;
    }

    public static String checkEmpty(String s){

        if (s.isEmpty()){
            s = String.valueOf(0);
        }
        return s;
    }
}
