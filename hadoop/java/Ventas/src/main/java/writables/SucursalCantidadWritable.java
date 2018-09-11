package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class SucursalCantidadWritable implements WritableComparable {

	private String sucursal;
	private Long cantidad;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.sucursal = WritableUtils.readString(in);
		this.cantidad = WritableUtils.readVLong(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, sucursal);
		WritableUtils.writeVLong(out, cantidad);
	}

	public SucursalCantidadWritable() {
	}

	public SucursalCantidadWritable(String sucursal, Long cantidad) {
		this.setSucursal(sucursal);
		this.setCantidad(cantidad);
	}

	public String getSucursal() {
		return sucursal;
	}

	public void setSucursal(String sucursal) {
		this.sucursal = sucursal;
	}

	public Long getCantidad() {
		return cantidad;
	}

	public void setCantidad(Long cantidad) {
		this.cantidad = cantidad;
	}

	@Override
	public String toString() {
		return (this.getSucursal() + "\t" + this.getCantidad());
	}

	@Override
	public int compareTo(Object o) {
		return this.toString().compareTo(((SucursalProductoWritable) o).toString());
	}
}
