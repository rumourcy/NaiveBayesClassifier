package io.github.trierbo.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class IndexCompute {
    // 选取BRAZ作为正例, CANA作为反例
    public static void main(String[] args) throws Exception {
        /*
         * matrix[0] --- TP
         * matrix[1] --- FN
         * matrix[2] --- TN
         * matrix[3] --- FP
         */
        int[] matrix = new int[4];
        File predict = new File("dataset/predict.txt");
        try (BufferedReader reader = new BufferedReader(new FileReader(predict))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] classification = line.split("\t");
                String[] filename = classification[0].split("/");
                String country = filename[filename.length - 2];
                String prediction = classification[1];
                if (country.equals("BRAZ")) {
                    if (country.equals(prediction))
                        matrix[0]++;
                    else
                        matrix[1]++;
                } else {
                    if (country.equals(prediction))
                        matrix[2]++;
                    else
                        matrix[3]++;
                }
            }
        }

        double precision = (double) matrix[0] / (matrix[0] + matrix[3]);
        double recall = (double) matrix[0] / (matrix[0] + matrix[1]);
        double F1 = (double) (2 * matrix[0]) / (2 * matrix[0] + matrix[3] + matrix[1]);

        System.out.println("-----------------------------------------------------");
        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F1: " + F1);
        System.out.println("-----------------------------------------------------");
    }
}
