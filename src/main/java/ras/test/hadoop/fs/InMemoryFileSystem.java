/**
 * Copyright 2013 Red Arch Solutions, Incorporated
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ras.test.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * An in-memory implementation of a {@link FileSystem}. This class was written
 * to support testing of code developed for the Hadoop environment. The file
 * system state is managed statically. This means that all instances of this
 * class share the same static file system state. Instances in two different
 * threads will share the same file system state. The file system is not
 * entirely thread safe. Access to the top level static file system state is
 * synchronized. This ensures that two instances in different threads don't
 * result in two completely different static instances of the file system state.
 * However, if these same instances attempt to modify the same directory or file
 * at the same time, bad things can and probably will happen.
 * 
 * 
 * This file system supports the concepts of a user, user groups and
 * permissions. The default user is 'root', the default group is 'test' and the
 * default permission is '777'. There is no support for the application of a
 * 'umask'.
 * 
 * This file system does not support monitoring progress through the
 * {@link Progressable} interface.
 * 
 * This file system does not support symbolic links.
 * 
 * Details on permissions:
 * http://hadoop.apache.org/docs/r1.0.4/hdfs_permissions_guide.html
 * 
 * TODO: Fix permission denied message to include path
 * 
 * TODO: Change the default file permission to 666
 * 
 * TODO: Test rename() with open file
 * 
 */
public class InMemoryFileSystem extends FileSystem {

	/** The scheme for this file system, "memory". */
	public static final String SCHEME = "memory";

	/** The URI for this file system. */
	public static final URI NAME = URI.create(SCHEME + ":///");

	/** This is the configuration key for the default file system name. */
	public static final String FS_DEFAULT_NAME_KEY = "fs.default.name";

	/**
	 * The {@link Configuration} key for this file systems implementation class.
	 */
	public static final String CONFIG_IMPL_CLASS_KEY = "fs." + SCHEME + ".impl";

	/** The permission used when none is specified, '777'. */
	public static final short DEFAULT_PERMISSION = 0777;

	/** The default user of the file system, 'root'. */
	public static final String DEFAULT_USER = "root";

	/** The default group of files and directories, 'test'. */
	public static final String DEFAULT_GROUP = "test";

	/** The default users groups, 'test'. */
	public static final Set<String> DEFAULT_USERS_GROUPS = Collections
			.unmodifiableSet(new HashSet<String>(Arrays.asList(DEFAULT_GROUP)));

	/** The root directory path. */
	private static final Path ROOT_PATH = new Path("/");

	/**
	 * A mapping from a path to a node in the file system. This is static shared
	 * state between all instances of this file system. No instance methods
	 * should access this variable directly.
	 */
	private static Map<URI, Map<String, Node>> fileSystemState = null;

	/**
	 * @return The file system state for the current thread.
	 */
	private static synchronized Map<String, Node> getPathMap(URI fsName) {
		if (fileSystemState == null) {
			fileSystemState = new HashMap<URI, Map<String, Node>>();
		}
		Map<String, Node> pathMap = fileSystemState.get(fsName);
		if (pathMap == null) {
			pathMap = new HashMap<String, Node>();
			pathMap.put(ROOT_PATH.toUri().getPath(), new DirectoryNode(
					ROOT_PATH, new FsPermission(DEFAULT_PERMISSION)));
			fileSystemState.put(fsName, pathMap);
		}
		return pathMap;
	}

	private static Node getPathMapNode(URI fsName, Path path) {
		return getPathMap(fsName).get(path.toUri().getPath());
	}

	private static void setPathMapNode(URI fsName, Path path, Node node){
		getPathMap(fsName).put(path.toUri().getPath(), node);
	}
	
	private static boolean containsNode(URI fsName, Path path){
		return getPathMap(fsName).containsKey(path.toUri().getPath());
	}
	
	private static void removeNode(URI fsName, Path path){
		getPathMap(fsName).remove(path.toUri().getPath());
	}
	
	/**
	 * Clears the entire file system state. This method affects ALL instance of
	 * the file system.
	 */
	public static synchronized void resetFileSystemState() {
		fileSystemState = null;
	}

	/**
	 * Sets up the <code>conf</code> argument to use this file system as the
	 * default so that subsequent calls to methods such as
	 * {@link FileSystem#get(Configuration)} will return an instance of this
	 * file system.
	 * 
	 * @param conf
	 *            The configuration to be modified to use this file system as
	 *            the default.
	 */
	public static void configure(Configuration conf) {
		Validate.notNull(conf, "conf == null not allowed!");

		conf.set(FS_DEFAULT_NAME_KEY, NAME.toString());
		conf.set(CONFIG_IMPL_CLASS_KEY, InMemoryFileSystem.class.getName());
	}

	/**
	 * A helper method that modifies the <code>conf</code> argument to use this
	 * file system as the default, creates and instance of the file system and
	 * initializes it with the specified configuration instance.
	 * 
	 * @param conf
	 *            The configuration to be modified and used by the file system.
	 * @return An instance of this file system initialized with the specified
	 *         configuration.
	 */
	public static InMemoryFileSystem get(Configuration conf) {
		configure(conf);
		InMemoryFileSystem imFs = new InMemoryFileSystem();
		imFs.setConf(conf);
		return imFs;
	}

	static void copy(InMemoryFileSystem srcFs, Path srcPath,
			InMemoryFileSystem dstFs, Path dstParentPath) throws IOException {

		FileUtil.copy(srcFs, srcPath, dstFs, dstParentPath, false, false,
				new Configuration());
	}

	/**
	 * A simple helper method for testing which creates a file in the file
	 * system
	 * 
	 * @param path
	 *            The path of the file to be created.
	 * @param contents
	 *            the contents to be stored in the file.
	 * @throws IOException
	 */
	public static void createFile(Path path, String contents)
			throws IOException {
		InMemoryFileSystem fs = InMemoryFileSystem.get(new Configuration());
		FSDataOutputStream out = fs.create(path);
		try {
			out.writeBytes(contents);
		} finally {
			out.close();
			fs.close();
		}
	}

	//
	// File System instance state...
	//

	private final URI name;

	/** The current file system user. */
	private String user = DEFAULT_USER;

	/** The current users group assignments. */
	private Set<String> userGroups = DEFAULT_USERS_GROUPS;

	/**
	 * The current working directory, initialized to '/' at file system
	 * construction.
	 */
	private Path workingDirectory = ROOT_PATH;

	/**
	 * Creates an instance of the in-memory file system.
	 */
	public InMemoryFileSystem() {
		this.name = NAME;
	}

	public InMemoryFileSystem(String scheme) {
		this.name = URI.create(scheme + ":///");
	}

	/**
	 * @return the URI for this file system.
	 */
	@Override
	public URI getUri() {
		return this.name;
	}

	/**
	 * Attempts to retrieve a node within the file system.
	 * 
	 * @param path
	 *            The path to the file system node.
	 * @param mustExist
	 *            A flag indicating whether the node must exist. If
	 *            <code>true</code> and the path does not map to an existing
	 *            node then an {@link IOException} is thrown.
	 * @return The node to which <code>path</code> maps or <code>null</code> if
	 *         no such mapping exists.
	 * 
	 * @throws IOException
	 *             If <cod>mustExist == true</code> and no node is found mapped
	 *             to <code>path</code>
	 */
	private Node getNode(Path path, boolean mustExist) throws IOException {
		Validate.notNull(path, "path == null not allowed!");
		Node node = getPathMapNode(name, path);
		if (mustExist && (node == null)) {
			throw new IOException("'" + path + "' not found!");
		}
		return node;
	}

	/**
	 * Attempts to retrieve a file node within the file system.
	 * 
	 * @param path
	 *            The path to the file node.
	 * @param mustExist
	 *            A flag indicating whether the node must exist. If
	 *            <code>true</code> and the path does not map to an existing
	 *            node then an {@link IOException} is thrown.
	 * @return The node to which <code>path</code> maps or <code>null</code> if
	 *         no such mapping exists.
	 * 
	 * @throws IOException
	 *             If <cod>mustExist == true</code> and no node is found mapped
	 *             to <code>path</code>.
	 * @throws IOException
	 *             If <code>path</code> maps to a directory node.
	 */
	private FileNode getFileNode(Path path, boolean mustExist)
			throws IOException {
		Node node = getNode(path, mustExist);
		if ((node != null) && !(node instanceof FileNode)) {
			throw new IOException("'" + path + "' is not a file!");
		}
		return (FileNode) node;
	}

	private Path makeAbsolute(Path path) {
		Validate.notNull(path, "path == null not allowed!");

		if (path.isAbsolute()) {
			return path;
		}
		return new Path(workingDirectory, path);
	}

	/**
	 * Opens a file for reading.
	 * 
	 * @param path
	 *            The path to the file
	 * @param buffersize
	 *            Ignored.
	 * 
	 * @throws IOException
	 *             If the file is not found or the path maps to a directory.
	 */
	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		path = makeAbsolute(path);
		FileNode node = getFileNode(path, true);
		checkPermission(node, FsAction.READ);
		return new FSDataInputStream(node.open());
	}

	/**
	 * Creates an output stream for the specified path.
	 * 
	 * Does it matter is the path is absolute or relative?
	 * 
	 * @param path
	 *            The path to the file to be created. May not be
	 *            <code>null</code>.
	 * @param permission
	 *            The file permissions. May be <code>null</code>. If
	 *            <code>null</code>, permissions will be set to '777'.
	 * @param overwrite
	 *            A flag indicating whether the file should be overwritten if it
	 *            already exists. if <code>false</code> and the file does exist
	 *            an {@link IOException} will be thrown.
	 * @param bufferSize
	 *            Ignored.
	 * @param replication
	 *            Ignored.
	 * @param blockSize
	 *            Ignored.
	 * @param progress
	 *            Ignored, may be <code>null</code>
	 */
	@Override
	public FSDataOutputStream create(Path path, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		path = makeAbsolute(path);
		String pathScheme = path.toUri().getScheme();
		if (pathScheme != null) {
			Validate.isTrue(
					name.getScheme().equals(pathScheme),
					"Wrong file system: " + pathScheme + ", expected: "
							+ name.getScheme());
		}
		checkParentDirWritePermission(path);
		Node node = getNode(path, false);

		if (node != null) {
			if (overwrite) {
				if (node instanceof DirectoryNode) {
					throw new IOException(
							"Can't overwrite a directory with a file: " + path);
				}
			} else {
				throw new IOException("File already exists: " + path);
			}
		}

		Path parentPath = path.getParent();
		mkdirs(parentPath, permission);

		FileNode fnode = new FileNode(path, permission);
		fnode.setOwner(user);
		setPathMapNode(name, path, fnode);

		DirectoryNode dnode = (DirectoryNode) getPathMapNode(name, parentPath);
		dnode.addFile(path);

		Statistics stats = new Statistics(workingDirectory.toUri().getScheme());
		return new FSDataOutputStream(fnode.append(), stats);
	}

	/**
	 * Opens an existing file for appending.
	 * 
	 * @param path
	 *            The path to an existing file.
	 * @param bufferSize
	 *            Ignored.
	 * @param progress
	 *            Ignored.
	 */
	@Override
	public FSDataOutputStream append(Path path, int bufferSize,
			Progressable progress) throws IOException {
		path = makeAbsolute(path);
		FileNode fnode = getFileNode(path, true);
		checkPermission(fnode, FsAction.WRITE);
		Statistics stats = new Statistics(workingDirectory.toUri().getScheme());
		return new FSDataOutputStream(fnode.append(), stats);
	}

	/**
	 * Renames a path resource (file or directory)
	 * 
	 * @param src
	 *            The path to the resource to be renamed. The path must exist.
	 * @param dst
	 *            The new path to for the resource. Must not already exist.
	 */
	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		Validate.notNull(src, "src == null not allowed!");
		Validate.notNull(dst, "dst == null not allowed!");
		src = makeAbsolute(src);
		dst = makeAbsolute(dst);

		if (getNode(dst, false) != null) {
			throw new IOException("Rename failed, destination already exists: "
					+ dst);
		}

		Node snode = getNode(src, true);
		checkPermission(snode, FsAction.WRITE);
		checkParentDirWritePermission(dst);

		Path pathToDst = dst;
		if (snode instanceof FileNode) {
			pathToDst = dst.getParent();
		}
		mkdirs(pathToDst);

		snode.setPath(dst);
		removeNode(name, src);
		setPathMapNode(name, dst, snode);

		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.fs.FileSystem#delete(org.apache.hadoop.fs.Path)
	 */
	@Override
	@Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	/**
	 * Deletes the resource identified by the specified path. If the resource is
	 * a directory and <code>recursive == false</code>, it must be empty
	 * otherwise an IOException will be thrown. If the resource is a directory
	 * and <code>recursive == true</code> then the directory and its contents
	 * will be deleted. If the resource is a file, then <code>recursive</code>
	 * is ignored.
	 * 
	 * If an open file is encountered during the recursive deletion of a
	 * directory, the delete will immediately terminate and throw an
	 * IOException. Some files and sub-directories may have been successfully
	 * deleted while other other may still remain. In other words, the file
	 * system will be in an unknown state.
	 * 
	 * @param path
	 *            The path to the resource which is to be deleted.
	 * @param recursive
	 *            A flag indicating whether a dirctory's content should be
	 *            deleted.
	 */
	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		path = makeAbsolute(path);
		Node node = getNode(path, true);
		checkPermission(node, FsAction.WRITE);
		if (node instanceof DirectoryNode) {
			if (recursive) {
				for (Path childDirPath : ((DirectoryNode) node)
						.getSubDirectories()) {
					delete(childDirPath, recursive);
				}

				for (Path childFile : ((DirectoryNode) node).getFiles()) {
					FileNode fnode = getFileNode(childFile, true);
					if (fnode.isOpen()) {
						throw new IOException(
								"Delete failed, resource is in use: "
										+ childFile);
					}
					removeNode(name, childFile);
				}
			} else {
				if (((DirectoryNode) node).getSubDirectories().size() > 0) {
					throw new IOException("Directory '" + path
							+ "' is not empty!");
				}
			}
		} else if (((FileNode) node).isOpen()) {
			throw new IOException("Delete failed, resource is in use: " + path);
		}
		removeNode(name, path);
		return true;
	}

	/**
	 * @param path
	 *            the path to be processed.
	 * 
	 * @return the file status for <code>path</code> if it is a file, otherwise
	 *         the list of file statuses for the files and directories under the
	 *         specified <code>path</code>.
	 */
	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		path = makeAbsolute(path);
		Node node = getNode(path, true);
		checkPermission(node, FsAction.READ);
		if (node instanceof FileNode) {
			return new FileStatus[] { getFileStatus(path) };
		}

		DirectoryNode dnode = (DirectoryNode) node;
		List<FileStatus> list = new ArrayList<FileStatus>();
		addStatus(list, dnode.getSubDirectories());
		addStatus(list, dnode.getFiles());
		return list.toArray(new FileStatus[list.size()]);
	}

	private void addStatus(List<FileStatus> list, Set<Path> paths)
			throws IOException {
		for (Path path : paths) {
			list.add(getFileStatus(path));
		}
	}

	@Override
	public void setWorkingDirectory(Path path) {
		path = makeAbsolute(path);
		try {
			Node node = getNode(path, true);
			if (node instanceof FileNode) {
				throw new IllegalArgumentException("'" + path
						+ "' is not a directory!");
			}
		} catch (IOException e) {
			/*
			 * This is annoying! The API does not allow us to throw an
			 * IOException which would be the consistent thing to do.
			 */
			throw new IllegalArgumentException("'" + path + "' not found!");
		}

		this.workingDirectory = path;
		checkPath(workingDirectory);
	}

	/**
	 * @return The current working directory.
	 */
	@Override
	public Path getWorkingDirectory() {
		return this.workingDirectory;
	}

	/**
	 * Climb the path back up until we encounter the first non-null parent
	 * directory. This directory will determine the permission for the
	 * operation. Note that this method assumes that the path has been made
	 * absolute and is rooted. Otherwise, it's an infinite loop.
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private DirectoryNode findNonNullParentDirectory(Path path)
			throws IOException {
		DirectoryNode parentDir = null;
		Path parentPath = path.getParent();
		do {
			parentDir = (DirectoryNode) getNode(parentPath, false);
			parentPath = parentPath.getParent();
		} while (parentDir == null);
		return parentDir;
	}

	private void checkPermission(Node node, FsAction action) throws IOException {
		boolean permissionDenied = false;
		FsPermission parentPermissions = node.getPermissions();
		// if world doesn't have permission...
		if (!parentPermissions.getOtherAction().implies(action)) {
			// if group doesn't have permission...
			if (!parentPermissions.getGroupAction().implies(action)) {
				// if the owner doesn't have permission or the user is
				// not the owner...
				if (!parentPermissions.getUserAction().implies(action)
						|| !node.getOwner().equals(user)) {
					permissionDenied = true;
				}
			} else if (!userGroups.contains(node.getGroup())) {
				permissionDenied = true;
			}
		}
		if (permissionDenied) {
			throw new IOException("Permission denied!");
		}
	}

	private void checkParentDirWritePermission(Path path) throws IOException {
		DirectoryNode parentDir = findNonNullParentDirectory(path);
		if (parentDir != null) {
			checkPermission(parentDir, FsAction.WRITE);
		}
	}

	/**
	 * Creates a directory structure
	 * 
	 * @param path
	 *            the directory path to be created.
	 * @param permission
	 *            the permission for each directory in the path. Defaults to
	 *            "777" if <code>null</code>.
	 */
	@Override
	public boolean mkdirs(Path path, FsPermission permission)
			throws IOException {
		path = makeAbsolute(path);
		if (containsNode(name, path)) {
			return true;
		}
		checkParentDirWritePermission(path);
		Path parentPath = path.getParent();
		DirectoryNode dnode = new DirectoryNode(path, permission);
		dnode.setOwner(user);

		while (parentPath != null && !containsNode(name, parentPath)) {
			mkdirs(parentPath, permission);
			DirectoryNode pNode = (DirectoryNode) getPathMapNode(name,
					parentPath);
			pNode.addSubDirectory(path);
		}

		setPathMapNode(name, path, dnode);
		return true;
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		path = makeAbsolute(path);
		Node node = getPathMapNode(name, path);
		if (node == null) {
			throw new FileNotFoundException("'" + path + "' not found!");
		}
		long length = 0;
		boolean isDir = true;
		if (node instanceof FileNode) {
			length = ((FileNode) node).bytes.length;
			isDir = false;
		}

		FileStatus status = new FileStatus(length, isDir, 1, 0, 0, 0,
				node.getPermissions(), node.getOwner(), node.getGroup(), path);
		return status;
	}

	/**
	 * Set owner of a path (i.e. a file or a directory). The parameters username
	 * and groupname cannot both be null.
	 * 
	 * @param path
	 *            The path
	 * @param username
	 *            If it is null, the original username remains unchanged.
	 * @param groupname
	 *            If it is null, the original groupname remains unchanged.
	 */
	@Override
	public void setOwner(Path path, String username, String groupname)
			throws IOException {
		path = makeAbsolute(path);
		Node node = getNode(path, true);
		node.setOwner(username);
		node.setGroup(groupname);
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @return the groups which the current file system user is assigned too.
	 */
	public Set<String> getUserGroups() {
		return userGroups;
	}

	/**
	 * @param user
	 *            the file system user name.
	 * @param groups
	 *            the groups in which the user is a member.
	 */
	public void setUser(String user, String... groups) {
		Validate.notNull(user, "user == null not allowed!");
		Validate.notNull(groups, "groups == null not allowed!");
		this.user = user;
		if (groups.length > 0) {
			Set<String> set = new HashSet<String>();
			for (String group : groups) {
				Validate.notNull(group, "groups[i] == null not allowed!");
				Validate.notEmpty(group, "groups[i].length() == 0 not allowed!");
				set.add(group);
			}
			this.userGroups = Collections.unmodifiableSet(set);
		}
	}
}
