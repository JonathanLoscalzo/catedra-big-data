package writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ProductoCantidadWritable implements WritableComparable {

	private String producto;
	private Long cantidad;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.producto = WritableUtils.readString(in);
		this.cantidad = WritableUtils.readVLong(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, producto);
		WritableUtils.writeVLong(out, cantidad);
	}

	public ProductoCantidadWritable() {
	}

	public ProductoCantidadWritable(String producto, Long cantidad) {
		this.setProducto(producto);
		this.setCantidad(cantidad);
	}

	public String getProducto() {
		return producto;
	}

	public void setProducto(String producto) {
		this.producto = producto;
	}

	public Long getCantidad() {
		return cantidad;
	}

	public void setCantidad(Long cantidad) {
		this.cantidad = cantidad;
	}

	@Override
	public String toString() {
		return (this.getProducto() + "\t" + this.getCantidad());
	}

	@Override
	public int compareTo(Object o) {
		return this.toString().compareTo(((ProductoCantidadWritable) o).toString());
	}
}