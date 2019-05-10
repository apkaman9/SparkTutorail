package com.virtualpairprogrammers;

/* 
 * Enter your code here. Read input from STDIN. Print your output to STDOUT. 
 * Your class should be named CandidateCode.
*/

import java.io.*;
import java.util.*;
public class CandidateCode {
    public static void main(String args[] ) throws Exception {
    	Scanner sc=new Scanner(System.in);
    	int T,N;
    	StringBuffer result=new StringBuffer();
        T=sc.nextInt();
        if(T>=1 && T<=10){
            for(int i=0;i<T;i++){
                N=sc.nextInt();
                if(N>=1 && N<=10000){
                    Integer[] integers=new Integer[N];
                    for(int j=0;j<N;j++){
                        integers[j]=sc.nextInt();
                    }
                    if(i!=0){
                        result.append("\n");
                    }
                    result.append(giftSum(integers, N));
                }
            }
        }
        System.out.println(result);
        sc.close();
   }
    private static String giftSum(Integer[] integers,int N) {
    	String max1=null;
    	int maxsum1=Collections.max(Arrays.asList(integers));
    	String max=String.valueOf(maxsum1);
    	for(int i=N-1;i>=0;i--) {
    		for(int j=i-2;j>=0;j--) {
    			int sum=integers[j]+integers[i];
    			if(sum>=maxsum1 && max1 ==null) {
    				int m=Integer.parseInt(integers[i]+""+integers[j]);
    				if(m>maxsum1) {
    					maxsum1=sum;
    					max1=integers[i]+""+integers[j];
    				}
    			}else if(sum>=maxsum1 && max1 !=null) {
    				int m=Integer.parseInt(integers[i]+""+integers[j]);
    				int n=Integer.parseInt(max1);
    				if(m>n) {
    					maxsum1=sum;
    					max1=integers[i]+""+integers[j];
    				}
    			}
    		}
    	}
    	if(max1!=null) {
    		max=max1;
    	}
    	return max;
    }
    private static void maxi() {

    	Scanner sc=new Scanner(System.in);
        int t,N;
        StringBuffer result=new StringBuffer();
        t=sc.nextInt();
        if(t>=0 && t<=10){
            for(int i=0;i<t;i++){
                N=sc.nextInt();
                int[] energy=new int[N];
                int[] strength=new int[N];
                for(int j=0;j<N;j++){
                    strength[j]=sc.nextInt();
                }
                for(int k=0;k<N;k++){
                    energy[k]=sc.nextInt();
                }
                if(maxiresult(energy,strength,N)){
                    if(i!=0){
                        result.append("\n");
                    }
                    result.append("WIN");
                }else{
                    if(i!=0){
                        result.append("\n");
                    }
                    result.append("LOSE");
                }

            }
        }
        System.out.println(result);
        sc.close();

   }
   private static boolean maxiresult(int[] energy,int[] strength,int N){
       Arrays.sort(energy);
       Map<Integer,Integer> result=new HashMap<Integer,Integer>();
       for(int i=0;i<N;i++){
           int e=energy[i];
           boolean res=false;
           for(int j=0;j<N;j++){
               if(!result.containsKey(j)) {
            	   if(strength[j]<e) {
            		   result.put(j, i);
            		   res=true;
            	   }
               }
           }
           if(!res) {
        	   return false;
           }
       }
       return true;
   }
}
