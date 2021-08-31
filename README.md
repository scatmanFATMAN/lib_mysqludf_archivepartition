<h1>lib_mysqludf_archivepartition</h1>
<p>MySQL User Defined Function for moving and managing partitions on disk.</p>
<p>MySQL doesn't offer a way to to change the data directory of a partition once the partition is already created. I had a need to keep many partitions, but older partitions moved off the SSDs and onto the slower HDDs. This library uses the same technique that MySQL uses when you specify a different data directory when creating a partition.</p>

<table>
  <tr>
    <th>Function</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>MOVE_PARTITION(user, password, database, table, partition, data_directory)</td>
    <td>Moves the partition to the data_directory specified as the last parameter. This function works by moving the partition to the new data directory and creating a soft link in place of the original partition file. While the partition is moving moved, a lock write will be acquired on the table. The return value be will <code>OK</code> on success, otherwise an error message is returned.<br /><br />Note: The password is in clear text and may be written to logs. I haven't figured out a better way to do this yet.</td>
  </tr>
</table>

<h3>Features</h3>
<ul>
  <li>Moving partitions across mount points and different filesystems.</li>
</ul>

<h3>Installing</h3>
<p>Simply run <code>sudo make install</code>. This will first build the library, then it'll install it in MySQL's plugin directory, and finally issue the mysql binary and run the SQL statements to install the functions. You will be prompted for your MySQL user and password.</p>

<h3>TODO</h3>
<ul>
  <li>Figure out a better way to pass the MySQL credentials. Maybe pass a file path to a config file as the first argument which includes username and password instead.</li>
  <li>Write a function to drop the partition. Using MySQL's DROP PARTITION will drop the partition, but keep the symbolic link.</li>
  <li>Support Windows, maybe.</li>
</ul>
