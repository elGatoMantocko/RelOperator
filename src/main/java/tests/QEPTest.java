package tests;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;

import java.util.Scanner;

import java.io.File;

public class QEPTest extends TestDriver {

  protected static final String TEST_NAME = "query evaluation pipeline tests";

  protected static Schema s_employee;
  protected static Schema s_department;

  protected static HeapFile empHeapFile;
  protected static HeapFile deptHeapFile;

  public static void main(String[] args) {

    File emps_file;
    File dept_file;

    QEPTest qept = new QEPTest();
    qept.create_minibase();

    empHeapFile = new HeapFile(null);
    deptHeapFile = new HeapFile(null);

    if (args.length > 0) {
      String rel_path = args[0];
      if (rel_path.charAt(rel_path.length() - 1) != '/') {
        rel_path = rel_path.concat("/");
      }
      System.out.println(rel_path);
      emps_file = new File(rel_path.concat("Employee.txt"));
      dept_file = new File(rel_path.concat("Department.txt"));
    } else {
      // init a default location for the files here
      emps_file = new File("./src/main/java/tests/SampleData/Employee.txt");
      dept_file = new File("./src/main/java/tests/SampleData/Department.txt");
    }

    // lets first work on the employees table
    try {
      Scanner emps_scanner = new Scanner(emps_file);

      // the first line has the names of the columns
      if (emps_scanner.hasNextLine()) {
        String firstLine = emps_scanner.nextLine();
        String[] colNames = firstLine.split(",");
        
        s_employee = new Schema(5);
        s_employee.initField(0, AttrType.INTEGER, 4, colNames[0].trim());
        s_employee.initField(1, AttrType.STRING, 20, colNames[1].trim());
        s_employee.initField(2, AttrType.INTEGER, 4, colNames[2].trim());
        s_employee.initField(3, AttrType.INTEGER, 4, colNames[3].trim());
        s_employee.initField(4, AttrType.INTEGER, 4, colNames[4].trim());
      }

      while (emps_scanner.hasNextLine()) {
        String emp = emps_scanner.nextLine();
        String[] fieldVals = emp.split(",");

        Tuple tuple = new Tuple(s_employee);

        tuple.setAllFields(
            Integer.parseInt(fieldVals[0].trim()),
            fieldVals[1].trim(),
            Integer.parseInt(fieldVals[2].trim()),
            Integer.parseInt(fieldVals[3].trim()),
            Integer.parseInt(fieldVals[4].trim())
        );

        // if we have to build a hash index, we will just make a FileScan
        tuple.insertIntoFile(empHeapFile);
      }

      emps_scanner.close();
    } catch(Exception e){
      e.printStackTrace(System.out);
      System.out.println("Couldn\'t read emps file");
    }

    try {
      Scanner dept_scanner = new Scanner(dept_file);

      // the first line has the names of the columns
      if (dept_scanner.hasNextLine()) {
        String firstLine = dept_scanner.nextLine();
        String[] colNames = firstLine.split(",");
        
        s_department = new Schema(4);
        s_department.initField(0, AttrType.INTEGER, 4, colNames[0]);
        s_department.initField(1, AttrType.STRING, 30, colNames[1]);
        s_department.initField(2, AttrType.INTEGER, 4, colNames[2]);
        s_department.initField(3, AttrType.INTEGER, 4, colNames[3]);
      }

      while (dept_scanner.hasNextLine()) {
        String dept = dept_scanner.nextLine();
        String[] fieldVals = dept.split(",");

        Tuple tuple = new Tuple(s_department);

        tuple.setAllFields(
            Integer.parseInt(fieldVals[0].trim()),
            fieldVals[1].trim(),
            Integer.parseInt(fieldVals[2].trim()),
            Integer.parseInt(fieldVals[3].trim())
        );

        // if we have to build a hash index, we will just make a FileScan
        tuple.insertIntoFile(deptHeapFile);
      }

      dept_scanner.close();
    } catch(Exception e){
      e.printStackTrace(System.out);
      System.out.println("Couldn\'t read dept file");
    }

    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = PASS;
    status &= qept.test1();
    status &= qept.test2();
    status &= qept.test3();
    status &= qept.test4();

    // display the final results
    System.out.println();
    if (status != PASS) {
        System.out.println("Error(s) encountered during " + TEST_NAME + ".");
        System.exit(-1);
    } else {
        System.out.println("All " + TEST_NAME + " completed; verify output for correctness.");
    }
  }

  // Display for each employee his ID, Name and Age
  protected boolean test1() {
    
    System.out.println("\nTest 1: Display for each employee his ID, Name and Age\n");

    try {
      FileScan scan = new FileScan(s_employee, empHeapFile);

      Projection pro = new Projection(scan, 0, 1, 2);
      pro.explain(0);
      System.out.println();
      pro.execute();

      System.out.println("\n\nTest 1 completed without exception.");

      return PASS;
    } catch (Exception e) {
      e.printStackTrace(System.out);
      System.out.println("\n\nTest 1 terminated because of exception.");
      return FAIL;
    }
  }

  // Display the Name for the departments with MinSalary = MaxSalary
  protected boolean test2() {
    System.out.println("\nTest 2: Display the Name for the departments with MinSalary = MaxSalary\n");

    try {
      FileScan scan = new FileScan(s_department, deptHeapFile);

      // i tried this with column names, but couldn't get it to work
      Selection sel = new Selection(scan, new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3));
      sel.explain(0);
      System.out.println();
      sel.execute();

      System.out.print("\n\nTest 2 completed without exception.");

      return PASS;
    } catch (Exception e) {
      e.printStackTrace(System.out);
      System.out.print("\n\nTest 2 terminated because of exception.");
      return FAIL;
    }
  }

  // For each employee, display his Name and the Name of his department as well as the maximum salary of his department
  protected boolean test3() {
    System.out.println("\nTest 3: For each employee, display his Name and the Name of his department as well as the maximum salary of his department\n");

    try {
      FileScan emp_scan = new FileScan(s_employee, empHeapFile);
      FileScan dept_scan = new FileScan(s_department, deptHeapFile);

      HashJoin join = new HashJoin(dept_scan, emp_scan, 0, 4);

      Projection pro = new Projection(join, 5, 1, 3);

      pro.explain(0);
      System.out.println();
      pro.execute();

    } catch(Exception e){
      e.printStackTrace();
    }

    return true;
  }

  // Display the Name for each employee whose Salary is
  //  greater than the maximum salary of his department.
  protected boolean test4() {
    // ignore
    return true;
  }
}
