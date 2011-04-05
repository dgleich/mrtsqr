import java.io.*;

import java.util.Random;

import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.QR;

public class test {
    public static void main(String args[]) {
        int m = 10000;
        int n = 1000;
        
        System.out.println(
            "Testing QR factorization of " + m + "-by-" + n + " matrix");
               
        DenseMatrix A = new DenseMatrix(m,n);
        DenseMatrix B = new DenseMatrix(n,n);
        Random R = new Random();
        
        for (int j=0; j<A.numColumns(); ++j) {
            for (int i=0; i<A.numRows(); ++i) {
                A.set(i,j,R.nextGaussian());
            }
        }
        
        long start = System.currentTimeMillis();
        
        QR qr = QR.factorize(A);
        B.set(qr.getR());
        
        long end = System.currentTimeMillis();
        
        System.out.println(
            "Total time: " + (end - start) + " millisecs.");
        
    }
}
