# MapReduceJSqoopHelper
Library for spliting data imported with sqoop before exporting.
<br/><br/>
This is a great tool when data is imported with sqoop from a RDBMS to Hadoop, and later inserted from hadoop into a different RDBMS.
It is well known sqoop allows an entire file to be inserted into a table in RDBMS, but there isn't a way to split data to be exported
based on conditions.

So for example, if we have 125 files to be inserted into tables A, B, and C based on certain conditions, we could run this tool 
as follows:<br/><br/>

<i>hadoop jar MapReduceJSqoopHelper.jar &lt;package_name_of_class_to_be_generated&gt; &lt;file_containing_json_schema&gt; &lt;absolute_path_of_class_file&gt; &lt;absolute_path_of_.java_file&gt; &lt;field_to_apply_condition_on,<b>operator</b>,<b>value</b>,<b>absolute_path_of_output_file</b>,<b>cast_to</b>;...;&lt;last_condition_tuple&gt; &lt;absolute_path_input_1,absolute_path_input_2,...,absolute_path_input_n&gt; &lt;absolute_path_output_1,absolute_path_ouput_2,...,absolute_path_output_n&gt;</i>
<br/><br/>
Basically, the tool expects 7 arguments, which are described below:
<ul>
<li>&lt;package_name_of_class_to_be_generated&gt;: this is the package where .class file belongs. Remember that this class file will be generated by <a href="https://github.com/jsoft88/JCAvroSchemaClassBuilder">JCAvroSchemaBuilder</a> (based on <a href="https://github.com/jsoft88/JCClassBuilder">JCClassBuilder</a>) and package name denotes the newly generated class' package.</li>
<li>&lt;file_containing_json_schema&gt;: a file containing a schema in json.</li>
<li>&lt;absolute_path_of_class_file&gt;: absolute path where generated .class file will be written to.</li> 
<li>&lt;absolute_path_of_.java_file&gt;: absolute path where .java source file of generated class will be written to.</li>
<li>&lt;field_to_apply_condition_on,<b>operator</b>,<b>value</b>,<b>absolute_path_of_output_file</b>,<b>cast_to</b>;...;&lt;last_condition_tuple&gt;: This is a list of tuples separated by semi-colon. On the other hand, tuple elements are separated by commas.
  <ul>
    <li>field_to_apply_condition_on: this has to be a field inside schema and it must match exactly the name of the field. So for example, if the schema has a field Name: String, this field must be called Name. Using name or NAME will not work.</li>
    <li>operator: The available operators are the following: lt (less than), le (less than or equals), eq (equals), gt (greater than) or ge (greater than or equals)</li>
    <li>value: The value against which field_to_apply_condition_on will be compared.</li>
    <li>absolute_path_of_output_file: absolute path of file where records matching this condition will be written.</li>
    <li>cast_to: fully qualified name of the java class to which this field must be cast to. If no cast is needed, then use an underscore (_).</li>
  </ul>
</li> 
<li>&lt;absolute_path_input_1,absolute_path_input_2,...,absolute_path_input_n&gt;: A list of input files separated by comma.</li> 
<li>&lt;absolute_path_output_1,absolute_path_ouput_2,...,absolute_path_output_n&gt;: A list of output files. <b>Always keep in mind</b> that this must match those output files inside condition tuples. Therefore, use absolute paths here and double check that they match those used in conditions.</li>
</ul>
<br/>
Once the script is done, you will have as many files as output files were passed to the tool. You can later use this output files one by one, issuing 
<i>n</i> sqoop export commands.
<br/><br/>
Hope you like it! And feel free to improve the tool. =D
