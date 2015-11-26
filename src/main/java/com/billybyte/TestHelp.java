package com.billybyte;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TestHelp {
	static String test1(String str){
		if(str.length()>=3 && str.substring(0,2).equals("not"))return(str);
		return "not " + str;
	}
	
	public static String frontBack(String str) {
		
		  String p1 = str.substring(str.length()-1,str.length());
		  String p2 = str.substring(1,str.length()-1);
		  String p3 = str.substring(0,1);
		  return p1+p2+p3;
		}
	
	
	public static  boolean makeBricks(int small, int big, int goal) {
		
		  if(big>goal){
			  if(small==goal)return true;
			  return makeBricks(small-1,0,goal);
		  }
		  int r = small + big*5;
		  if(r==goal)return true;
		  if(big>0){
			  boolean t1 =  makeBricks(small,big-1,goal);
			  if(t1)return true;
		  }
		  if(small>0){
			  return makeBricks(small-1,big,goal);  
		  }
		  return false;
		  
		  
		}
	
	public static int loneSum(int a, int b, int c) {
		  if(a==b)return loneSum(0,b,c);
		  if(a==c)return loneSum(0,b,c);
		  if(b==c)return loneSum(a,b,0);
		  return a+b+c;
		}

	public static void main(String[] args) {
		String s = "abc a";
//		System.out.println(s.substring(0, 3));
//		System.out.println(s.substring(0, 3).equals("abc"));
//		System.out.println(frontBack("candy"));
//		System.out.println(frontBack("x"));
//		System.out.println(frontBack("not bad"));
		System.out.println(makeBricks(3,1,8));
		System.out.println(makeBricks(3,1,9));
		System.out.println(makeBricks(3,2,10));
		
		int[] ar = {1,2,3};
		System.out.println(loneSum(9,2,2));
		
        Scanner in = new Scanner(System.in);
        int t;
        int sum;
        int a,b;
        t = in.nextInt();
        for (int i=0;i<t;i++) {
            a = in.nextInt();
            b = in.nextInt();
            sum = a+b;
            System.out.println(sum);
        }
	
	}
}
