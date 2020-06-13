package com.stirperichard.stormbus.utils;

public class ParseTime {

    //Converto la colonna non omogenea in soli interi [minutes] <- misura std di riferimento
    public static String minutesDelayed(String delay) {

        String[] delays;
        int newDelay = 0;

        //Se è vuota o contiene "?", assegno 0 di default
        if (delay.isEmpty() || delay.equals("?") || delay.equals("!")){
            return delay = String.valueOf(0);
        }

        //Cerco di rendere la colonna dei delay quanto più omogena possibile
        delay = delay.replace(" ", "");
        delay = delay.replace(".", "");
        delay = delay.replace(",", "");
        delay = delay.replace("?", "");
        delay = delay.replace("!", "");
        delay = delay.replace("@", "");

        //Controllo se ha "-" oppure "/" -> SPLIT
        if (delay.contains("-") || delay.contains("/")){
            delays = delay.split("-/", -1);
            if (delays[0] == "1" && delays[1] == "2"){
                delay = String.valueOf(0.5);
            }

            int i;
            for(i = 0; i < delay.length(); i++){

                //Controllo se sono ORE
                if(delays[i].contains("h") || delays[i].contains("H")){
                    //Rimuovo le lettere
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    checkEmpty(delays[i]);

                    //Trasformo le ore in minuti
                    delays[i] = String.valueOf(Integer.parseInt(delays[i]) * 60);

                    newDelay += Integer.parseInt(delays[i]);

                } else if (delays[i].contains("m") || delays[i].contains("M")){
                    //Controllo se sono minuti
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    checkEmpty(delays[i]);

                    newDelay += Integer.parseInt(delays[i]);
                } else {
                    //Controllo se non ha lettere, assegno di default minuti
                    delays[i] = delays[i].replaceAll("[^0-9]+", "");

                    checkEmpty(delays[i]);

                    newDelay += Integer.parseInt(delays[i]);
                }
            }
            return String.valueOf(newDelay);
        }

        //CASO IN CUI NON ABBIA CARATTERI DI SPLIT
        //Caso in cui siano scritti consecutivi
        if ((delay.contains("h") || delay.contains("H")) && (delay.contains("m") || delay.contains("M"))){
            delay = delay.replaceAll("[^0-9]+", " ");
            String [] delaysS = delay.split(" ");
            for (int j = 0; j < delaysS.length; j++){
                newDelay += Integer.parseInt(String.valueOf(delaysS));
            }

        }
        //Se non contiene unità di misura -> minuti
        if (!((delay.contains("h") || delay.contains("H")) || (delay.contains("m") || delay.contains("M")))){
            delay = delay.replaceAll("[^0-9]+", "");
            checkEmpty(delay);
            newDelay = Integer.parseInt(delay);
        }
        //Se contiene solo ORE
        if ((delay.contains("h") || delay.contains("H")) && !(delay.contains("m") || delay.contains("M"))){
            delay = delay.replaceAll("[^0-9]+", "");
            checkEmpty(delay);
            newDelay = Integer.parseInt(delay)*60;
        }
        //Se contiene solo MINUTI
        if (!(delay.contains("h") || delay.contains("H")) && (delay.contains("m") || delay.contains("M"))){
            delay = delay.replaceAll("[^0-9]+", "");
            checkEmpty(delay);
            newDelay = Integer.parseInt(delay);
        }

        return String.valueOf(newDelay);
    }

    public static String checkEmpty(String s){
        if (s.isEmpty()){
            s = String.valueOf(0);
        }
        return s;
    }
}
