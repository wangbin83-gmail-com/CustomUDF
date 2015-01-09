package com.jd.bi.hive.udfs.test;

import java.util.ArrayList;
import java.util.List;

public class Solution {
	public static class TreeNode {
	    int val;
	    TreeNode left;
	    TreeNode right;
	    TreeNode(int x) { val = x; }
    }
	public void flatten(TreeNode root) {
		while (root != null) {
			if (root.left != null) {
				TreeNode ptr = root.left;
				while (ptr.right != null) {
					ptr = ptr.right;
				}
				ptr.right = root.right;
				root.right = root.left;
				root.left = null;
			}
			root = root.right;
		}
	}
    public int sumNumbers(TreeNode root) {
        List<List<Integer>> result = new ArrayList<List<Integer>>();
        helper(root, result, new ArrayList());
        int all = 0;
        for (List<Integer> lst : result) {
            int path = 0;
            int tmp = 1;
            for (int i = lst.size() - 1; i >= 0; i--) {
                path += (lst.get(i) * tmp);
                tmp *= 10;
            }
            all += path;
        }
        return all;
    }
    private void helper(TreeNode node, List<List<Integer>> result, ArrayList<Integer> lst) {
    	lst.add(node.val);
        if (node.left == null && node.right == null) {
            result.add(lst);
            return;
        } 
        if (node.left != null) {
            helper(node.left, result, (ArrayList<Integer>)lst.clone());
        } 
        if (node.right != null) {
            helper(node.right, result, (ArrayList<Integer>)lst.clone());
        }
    }

    public List<List<Integer>> combine(int n, int k) {
        List<List<Integer>> result = new ArrayList<List<Integer>>();
        combine(n, k, 1, result, new ArrayList<Integer>());
        return result;
    }
    
    public void combine(int n, int k, int start, List<List<Integer>> result, ArrayList<Integer> lst) {
        if (k == 0) {
           result.add(lst);
           return;
        }
        for (int i = start; i <= n; i++) {
            ArrayList<Integer> a = (ArrayList<Integer>)lst.clone();
            a.add(i);
            combine(n, k-1, i + 1, result, a);
        }
    }
    public List<Integer> getRow2(int rowIndex) {
        int k = rowIndex;
        List<Integer> old_lst = new ArrayList<Integer>(k + 1);
        List<Integer> new_lst = new ArrayList<Integer>(k + 1);
        for (int i = 0; i < k + 1; i++) {
            old_lst.add(0);
            new_lst.add(0);
        }
        old_lst.set(0, 1);
        for (int j = 1; j < k + 1; j++) {
            new_lst.set(0, old_lst.get(0));
            new_lst.set(j, old_lst.get(j-1));
            for (int i = 1; i < j; i++) {
                new_lst.set(i, old_lst.get(i - 1) + old_lst.get(i));
            }
            List<Integer> tmp = old_lst;
            old_lst = new_lst;
            new_lst = tmp;
        }
        return old_lst;
    }
    public List<Integer> getRow(int rowIndex) {
        int k = rowIndex;
        int[] old_lst = new int[k+1];
        old_lst[0] = 1;
        for (int j = 1; j < k + 1; j++) {
        	int[] new_lst = new int[k+1];
            new_lst[0] = old_lst[0];
            new_lst[j] = old_lst[j-1];
            for (int i = 1; i < j; i++) {
                new_lst[i] = old_lst[i - 1] + old_lst[i];
            }
            int[] tmp = new_lst;
            old_lst = new_lst;
            new_lst = tmp;
        }
        List<Integer> r = new ArrayList<Integer>();
        for (int i = 0; i < old_lst.length; i++) {
        	r.add(old_lst[i]);
        }
        return r;
    }
    
    public static void main(String[] argv) {
    	TreeNode root = new TreeNode(1);
    	TreeNode left = new TreeNode(2);
    	TreeNode right = new TreeNode(5);
    	root.left = left;
    	root.right = right;
    	Solution s = new Solution();
    	System.out.println(s.sumNumbers(root));
//    	List<List<Integer>> lst = s.combine(5, 3);
//    	StringBuffer sb = new StringBuffer();
//    	for (int i = 0; i < lst.size(); i++) {
//    		for (int j = 0; j < lst.get(i).size(); j++) {
//    			sb.append(lst.get(i).get(j) + " ");
//    		}
//    		sb.append("\n");
//    	}
//    	System.out.println(sb.toString());
    }
}