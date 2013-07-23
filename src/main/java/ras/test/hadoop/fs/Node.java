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

import static ras.test.hadoop.fs.InMemoryFileSystem.DEFAULT_PERMISSION;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

abstract class Node {
	private Path path;
	private FsPermission permissions;
	private String owner = "root";
	private String group = "test";

	Node(Path path, FsPermission permissions) {
		Validate.notNull(path, "path == null not allowed!");
		this.path = path;
		this.setPermissions(permissions);
	}

	/**
	 * @return the path to this node
	 */
	public Path getPath() {
		return path;
	}

	public void setPath(Path path){
		this.path = path;
	}
	
	/**
	 * @return the permissions
	 */
	public FsPermission getPermissions() {
		return permissions;
	}

	/**
	 * @param permissions
	 *            the permissions to set
	 */
	public void setPermissions(FsPermission permissions) {
		if (permissions == null) {
			permissions = new FsPermission(DEFAULT_PERMISSION);
		}
		this.permissions = permissions;
	}

	/**
	 * @return the owner
	 */
	public String getOwner() {
		return owner;
	}

	/**
	 * @param owner the owner to set
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @return the group
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * @param group the group to set
	 */
	public void setGroup(String group) {
		this.group = group;
	}

}
