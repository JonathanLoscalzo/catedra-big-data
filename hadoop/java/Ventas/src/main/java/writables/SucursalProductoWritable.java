package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class SucursalProductoWritable implements WritableComparable {

	private String sucursal;
	private String producto;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.sucursal = WritableUtils.readString(in);
		this.producto = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, sucursal);
		WritableUtils.writeString(out, producto);
	}

	public SucursalProductoWritable() {
	}

	public SucursalProductoWritable(String sucursal, String producto) {
		this.setSucursal(sucursal);
		this.setProducto(producto);
	}

	public String getSucursal() {
		return sucursal;
	}

	public void setSucursal(String sucursal) {
		this.sucursal = sucursal;
	}

	public String getProducto() {
		return producto;
	}

	public void setProducto(String producto) {
		this.producto = producto;
	}

	@Override
	public String toString() {
		return (this.getSucursal() + "\t" + this.getProducto());
	}

	@Override
	public int compareTo(Object o) {
		return this.toString().compareTo(((SucursalProductoWritable) o).toString());
	}
}
