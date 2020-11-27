/** @module build */
import { Factory } from 'pip-services3-components-node';
import { Descriptor } from 'pip-services3-commons-node';

import { SqlServerConnection } from '../persistence/SqlServerConnection';

/**
 * Creates SqlServer components by their descriptors.
 * 
 * @see [[https://pip-services3-node.github.io/pip-services3-components-node/classes/build.factory.html Factory]]
 * @see [[SqlServerConnection]]
 */
export class DefaultSqlServerFactory extends Factory {
	public static readonly Descriptor: Descriptor = new Descriptor("pip-services", "factory", "sqlserver", "default", "1.0");
    public static readonly SqlServerConnectionDescriptor: Descriptor = new Descriptor("pip-services", "connection", "sqlserver", "*", "1.0");

    /**
	 * Create a new instance of the factory.
	 */
    public constructor() {
        super();
        this.registerAsType(DefaultSqlServerFactory.SqlServerConnectionDescriptor, SqlServerConnection);
    }
}
